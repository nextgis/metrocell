from os import path
import string
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
estimDf = path.join(project_path,"data\\stats\\prediction\\estimDf.csv")
# output for result frame of comparison of PowerCorrelation method of positioning
CorrResultsDf = path.join(project_path,"data\stats\\prediction\\\corrResultsDf.csv")
# predicted after estimation DataFrames
PredictedDf = path.join(project_path,"data\\stats\\prediction\\PredictedDf_PowCorrMethod.csv")
# input lines
segments_geojson_path = path.join(project_path,"segments\\raw\\msk\\metro_lines_dissolved_3857.geojson")
# output for plots
saveCellFolder = path.join(project_path,"data\\plots\\msk\\referenced\\by_cells\\")


def placeStrBeforeType(path,val):
    t = string.split(path,".")
    s2 = t[0] + "_" + val + "." + t[1]
    return s2
