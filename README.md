# shifting_work_hours

1. 获取带下载的文件信息。nasa\_climate\_data是爬虫工具scrapy写的脚本，是用来从nasa下载气候有关数据信息的。运行后得到的nasa\_climate\_data\_info.csv记录了准备下载的具体数据信息。
2. 下载文件。下载过程涉及两个python代码，分别是climate\_data\_downloader.py和rebuild\_metadata.py。以及一个window下bat脚本auto\_restart.bat。如果设置正确，双击auto\_restart.bat是能够直接下载，该bat脚本会运行climate\_data\_downloader.py进行下载，并且如果下载中断，会自动重新继续下载。主要设置是确定conda的位置。rebuild\_metadata.py脚本的作用是有可能下载中断，重建下载历史信息，确保不会重复下载。
3. 下载数据的目录结构：

    downloaded_data

     --EC-Earth3

         -- ssp126

             --  r1i1p1f1

                  --  hurs

                  --  rsds

                  --  sfcWind

                  --  tas

                  --  tasmax

         -- ssp245

           ...

     --GFDL-ESM4

          ...

     --IPSL-CM6A-LR

     -- NorESM2-MM
4. 裁切数据。根据需要裁切上述下载到的气候数据。本例中是中国中国经纬度包含的矩形面积的数据。china_bounds_file.json是经纬度信息。extract.py是裁切代码。正常情况，裁切速度受到硬盘IO速度和CPU线程数影响，调节代码中works=8，batchsize=2，对于本人机器速度最快。可以写代码测试12个文件，可知最优数。如果换成固态硬盘，可能硬盘IO不是关键因素，CPU线程数影响速度。尽量用固态速度会快很多。裁切数据可以单独设置位置。
