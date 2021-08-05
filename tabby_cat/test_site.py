"""
A PoC for the healthsite.io data
"""
import os
import logging
import sys

import click
import geopandas as gpd

from tabby_cat.data_loader import DataLoader
from tabby_cat.processor import Processor
from tabby_cat.solver import PCSTSolver


def write_stats(dir_path, name, data, file_type='csv'):
    data['lat'] = data.geometry.apply(lambda x: x.centroid.coords[0][0])
    data['lon'] = data.geometry.apply(lambda x: x.centroid.coords[0][1])
    data = data.to_crs('epsg:2163')
    data['length'] = data.geometry.apply(lambda x: x.length)
    data = data.to_crs('epsg:4326')

    if file_type == 'csv':
        data.to_csv(os.path.join(dir_path, name + "_network.csv"))


@click.command()
@click.option('--where', help='demand points to load')
@click.option('--demand', default=None, help='demand points to load')
@click.option('--additional_streets', default=None, help='street line data to load')
def main(where, demand, additional_streets):
    logging.basicConfig(filename='log.log',level=logging.DEBUG)

    traverse = 3
    node_gap = 9999
    two_edge_cost = 9999
    four_edge_cost = 9999
    n_edge_cost = 9999
    nearest_cost = 9999

    logging.info("Started DataLoader")
    dl = DataLoader()

    if demand:
        demand = gpd.read_file(demand)
        demand['geometry'] = demand.apply(lambda x: x.geometry.centroid, axis=1)
    else:
        dl.download_data_openaddress(where)
        demand = dl.address_df

    bbox = tuple(demand.total_bounds)
    print(demand.total_bounds, bbox)
    
    if additional_streets:
        additional_streets = gpd.read_file(additional_streets)
    else:
        logging.info("Started DataLoader: geofabrik")
        dl.download_data_geofabrik(where)
        logging.info("Reading street data")
        dl.read_street_data(where, bounds=bbox)

    logging.info(f"Running on {demand}")

    # where is this case is just a string used to name files
    pr = Processor(where)

    if additional_streets:
        logging.info("Snapping addresses to streets")
        pr.snap_points_to_line(additional_streets, demand)
    else:
        pr.snap_points_to_line(dl.streets_df, demand)
    
    logging.info("Converting GIS to graph")
    pr.geom_to_graph(
        traverse=traverse,
        node_gap=node_gap,
        two_edge_cost=two_edge_cost,
        four_edge_cost=four_edge_cost,
        n_edge_cost=n_edge_cost,
        nearest_cost=nearest_cost
    )
    logging.info("Writing intermediate files")
    pr.store_intermediate()

    logging.info("Create solver")
    sl = PCSTSolver(pr.edges, pr.look_up, pr.demand_nodes)
    logging.info("Running solve")
    sl.solve()

    pr.graph_to_geom(sl.s_edges)

    pr.solution.to_crs("epsg:4326").to_file(f"{pr.where}/output/solution.shp")

    write_stats(f"{pr.where}/output/", where, pr.solution)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
