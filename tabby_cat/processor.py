"""
Run the DataLoader dataframes through processing
"""
import os

import numpy as np
import pandas as pd
import geopandas as gpd
import multiprocessing as mp
from shapely.ops import split
from shapely.geometry import LineString, MultiPoint
from pyproj import Proj, transform

class Processor():
    def __init__(self, where):
        self.where = where
        self.snap_lines = None
        self.all_lines = None
        self.cut_lines = None
        self.demand = set()
        self.inProj = Proj(init='epsg:3857')
        self.outProj = Proj(init='epsg:4326')

    def _parallelize(self, points, lines):
        """
        Concept taken from here: https://swanlund.space/parallelizing-python
        """
        cpus = mp.cpu_count()
        
        intersection_chunks = np.array_split(points, cpus)
        
        pool = mp.Pool(processes=cpus)
        
        chunk_processes = [pool.apply_async(self._snap_part, args=(chunk, lines)) for chunk in intersection_chunks]

        intersection_results = [chunk.get() for chunk in chunk_processes]
        
        intersections_dist = pd.concat(intersection_results)

        return intersections_dist

    def _snap_part(self, gdf_chunk, lines):
        offset = 1000

        bbox = gdf_chunk.bounds + [-offset, -offset, offset, offset]
        hits = bbox.apply(lambda row: list(lines.sindex.intersection(row)), axis=1)

        tmp = pd.DataFrame({
            # index of points table
            "pt_idx": np.repeat(hits.index, hits.apply(len)),    # ordinal position of line - access via iloc later
            "line_i": np.concatenate(hits.values)
        })
        # Join back to the lines on line_i; we use reset_index() to 
        # give us the ordinal position of each line
        tmp = tmp.join(lines.reset_index(drop=True), on="line_i")
        # Join back to the original points to get their geometry
        # rename the point geometry as "point"
        tmp = tmp.join(gdf_chunk.geometry.rename("point"), on="pt_idx")
        # Convert back to a GeoDataFrame, so we can do spatial ops
        tmp = gpd.GeoDataFrame(tmp, geometry="geometry", crs=gdf_chunk.crs)

        tmp["snap_dist"] = tmp.geometry.distance(gpd.GeoSeries(tmp.point))
        # Discard any lines that are greater than tolerance from points
        tolerance = 100
        #tmp = tmp.loc[tmp.snap_dist <= tolerance]
        # Sort on ascending snap distance, so that closest goes to top
        tmp = tmp.sort_values(by=["snap_dist"])
        # group by the index of the points and take the first, which is the
        # closest line
        closest = tmp.groupby("pt_idx").first()

        # construct a GeoDataFrame of the closest lines
        return  gpd.GeoDataFrame(closest, geometry="geometry")

    def points_to_multipoint(self, data):
        coords = set()
        for p in data.snapped:
            coords.add(p.coords[0])
            self.demand.add(p.coords[0])

        return data.geometry.iloc[0].difference(MultiPoint(list(coords)).buffer(1e-7))

    def project_array(self, coordinates):
        """
        Project a numpy (n,2) array in projection srcp to projection dstp
        Returns a numpy (n,2) array.
        """

        fx, fy = pyproj.transform(self.inProj, self.outProj, coordinates[:,0], coordinates[:,1])
        # Re-create (n,2) coordinates
        return np.dstack([fx, fy])[0]

    def snap_points_to_line(self, lines, points, write=False):
        """
        Taken from here: https://medium.com/@brendan_ward/how-to-leverage-geopandas-for-faster-snapping-of-points-to-lines-6113c94e59aa
        """
        # this creates and also provides us access to the spatial index
        lines = lines.to_crs('epsg:3857')
        self.lines = lines
        points = points.to_crs('epsg:3857')

        closest = self._parallelize(points, lines)
        # Position of nearest point from start of the line
        series = gpd.GeoSeries(closest.point)
        series.crs = {'init': 'epsg:3857'}
        pos = closest.geometry.project(series)
        # Get new point location geometry
        new_pts = closest.geometry.interpolate(pos)
        # Identify the columns we want to copy from the closest line to the point, such as a line ID.
        line_columns = ['line_i', 'osm_id', 'code', 'fclass']
        # Create a new GeoDataFrame from the columns from the closest line and new point geometries (which will be called "geometries")
        snapped = gpd.GeoDataFrame(
            closest[line_columns],geometry=new_pts, crs="epsg:3857")
        closest['snapped'] = snapped.geometry
        split_lines = closest.groupby(closest["line_i"]).apply(lambda x: self.points_to_multipoint(x))
        split_lines_df = pd.DataFrame({"geom": split_lines})
        self.cut_lines = gpd.GeoDataFrame(split_lines_df, geometry="geom", crs="epsg:3857").to_crs('epsg:4326')
 
        # Join back to the original points:
        updated_points = points.drop(columns=["geometry"]).join(snapped)
        # You may want to drop any that didn't snap, if so: 
        updated_points = updated_points.dropna(subset=["geometry"]).to_crs('epsg:4326')
        


        if write:
            os.mkdir(f"{self.where}/output")
            updated_points.to_file(f"{self.where}/output/updated.shp")
            snap_lines = closest.apply(lambda x: LineString([x.point.coords[0], x.snapped.coords[0]]), axis=1)
            snap_df = pd.DataFrame({"geom": snap_lines})
            snap_gdf = gpd.GeoDataFrame(snap_df, geometry="geom", crs="epsg:3857")
            snap_gdf['length'] = snap_gdf.geometry.apply(lambda x: x.length)
            snap_gdf = snap_gdf.to_crs('epsg:4326')
            snap_gdf['lat'] = snap_gdf.geometry.apply(lambda x: x.coords[0][0])
            snap_gdf['lon'] = snap_gdf.geometry.apply(lambda x: x.coords[0][1])
            snap_gdf[["lat", "lon", "length"]].to_csv(f"{self.where}/output/connections.csv")
            snap_gdf.to_file(f"{self.where}/output/test_lines.shp")

    def set_node_ids_single_line(self, s, e):
        s_coord_string = f'[{s[0]:.1f}, {s[1]:.1f}]'
        e_coord_string = f'[{e[0]:.1f}, {e[1]:.1f}]'

        start = self.look_up.get(s_coord_string, None)
        if start is None:
            self.look_up[s_coord_string] = self.index
            start = self.index
            self.index += 1

        end = self.look_up.get(e_coord_string, None)
        if end is None:
            self.look_up[e_coord_string] =  self.index
            end = self.index
            self.index += 1

        self.edges.add((start, end))

    def set_node_ids_multi_line(self, s, e):
        start_nodes = []
        end_nodes = []
        for p in s:
            s_coord_string = f'[{p[0]:.1f}, {p[1]:.1f}]'
            start = self.look_up.get(s_coord_string, None)
            if start is None:
                self.look_up[s_coord_string] = self.index
                start_nodes.append(self.index)
                start = self.index
                self.index += 1
            else:
                start_nodes.append(start)

            start_nodes.append(start)

        for p in e:
            e_coord_string = f'[{p[0]:.1f}, {p[1]:.1f}]'
            end = self.look_up.get(e_coord_string, None)
            if end is None:
                self.look_up[s_coord_string] = self.index
                end_nodes.append(self.index)
                start = self.index
                self.index += 1
            else:
                end_nodes.append(end)

            end_nodes.append(end)

        self.edges.update(tuple(zip(start_nodes, end_nodes)))

    def geom_to_graph(self):
        self.lines["start"] = self.lines.geometry.apply(lambda x: x.coords[0])
        self.lines["end"] = self.lines.geometry.apply(lambda x: x.coords[-1])
        self.cut_lines["start"] = self.cut_lines.geometry.apply(lambda x: [geom.coords[0] for geom in x] if x.geom_type == "MultiLineString" else x.coords[0])
        self.cut_lines["end"] = self.cut_lines.geometry.apply(lambda x: [geom.coords[-1] for geom in x] if x.geom_type == "MultiLineString" else x.coords[-1])
        self.look_up = {}
        self.edges = set()
        self.index = 0
        self.lines.apply(lambda x: self.set_node_ids_single_line(x.start, x.end), axis=1)
        self.cut_lines.apply(lambda x: self.set_node_ids_multi_line(x.start, x.end), axis=1)

        import pdb; pdb.set_trace()

    def graph_to_geom(self):
        pass
