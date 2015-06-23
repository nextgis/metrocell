from os import path
"""
default paths
"""
project_path = "C:\\Users\\Alex\\Documents\\GitHub\\metrocell\\"
#input to averaging
inputMovesPath = path.join(project_path,"data\\referenced\\msk\\cell\\log_points.csv")
inputStopsPath = path.join(project_path,"data\\referenced\\msk\\cell\\log_points-stops.csv")
preLogPointsPath = path.join(project_path,"data\\referenced\\msk\\cell\\pre_log_points.csv")
preLogPointsPathStop = path.join(project_path,"data\\referenced\\msk\\cell\\pre_log_points-stops.csv")
    #test
testSegmentPath = path.join(project_path,"data\\118-227-2015031016.csv")
# output to averaging
saveCellSmoothed = path.join(project_path,"data\\Cells_smoothed_ref-200.csv")
output_db = path.join(project_path,"data")
tabname = "cell_log_points"
# output to subway information
subwayInfo = path.join(project_path,"data\\subwayInfo.csv")
# output to result of estimation
estimDf = path.join(project_path,"data\\estimDf.csv")
# input lines
segments_geojson_path = path.join(project_path,"segments\\raw\\msk\\metro_lines_dissolved_3857.geojson")
# output for plots
saveCellFolder = path.join(project_path,"data\\plots\\msk\\referenced\\by_cells\\")

