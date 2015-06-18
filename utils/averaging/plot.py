"""
Plot the parameter's distribution for each segment. There are many ways of representation:
it could be splitted by users,by lac-cid, etc.
"""
from ggplot import *
import paths
import pandas as pd
from preproc import  Preproc
class Plot():
    def __init__(self,saveFolder,ref = '',
                 plot_pars = {}
                 ):
        self.ref = ref
        self.plot_pars = plot_pars
        self.saveFolder = saveFolder
        return

    def by_segment(self,df):
        """

        Iterate each segment. Each one segment - one plot. For each plot might be added
        additional dimensions. For example:
        x-axis: ration
        y-axis: power
        different colors may shows different Users
        line types - network types and etc

        :df: dataframe
        :saveFolder: folder where will be pushed output plots


        :return: save plots to saveFolder path

        """
        plot_pars = self.plot_pars


        segments = list(df['segment'].drop_duplicates())

        for seg in segments:
            fname = seg + "-" + self.ref

            seg_df = df[df['segment'] == seg]
            p = ggplot(aes(x = plot_pars['aes']['x'],
                               y = plot_pars['aes']['y'],
                               color = plot_pars['color']),

                           data = seg_df)+\
                    ggtitle("Segment: " + list(seg_df['segment'].drop_duplicates())[0])
            p2 = p + geom_line(size = 1.5)+\
                     facet_grid(plot_pars['y_facet'],scales = plot_pars['facet_scales'])
            try:
                ggsave(filename = fname + '.png',plot = p2,path = self.saveFolder, width = 10)
            except:
                continue

if __name__ == '__main__':
    """
    # plots by segments
    powerByCells = Plot(saveFolder= paths.saveCellFolder,ref = 'stop',
                        plot_pars = {'y_facet':'laccid',
                                    'color':'User',
                                    'linetype':'NetworkGen',
                                    'facet_scales':'free_x',
                                    'aes':{'x':'ration','y':'Power'}
                                      }
                        )
    powerByUsers = Plot(saveFolder = paths.saveUserFolder,ref = 'stop',
                        plot_pars = {'y_facet':'User',
                                    'color':'laccid',
                                    'linetype':'NetworkGen',
                                    'facet_scales':'free_x',
                                    'aes':{'x':'ration','y':'Power'}
                                      }
                        )
    df = pd.io.parsers.read_csv(paths.inputStopsPath)
    preproc_ = Preproc()
    Df = preproc_.proc_cell_df(df,users = ['sasfeat'])
    powerByCells.by_segment(Df)
    powerByUsers.by_segment(Df)
    """
    #plot stats
    df = pd.io.parsers.read_csv(paths.saveStatsFolder + "Power.csv")
    powerStats = Plot(saveFolder = paths.saveStatsPlotsFolder)
    powerStats.stats()
