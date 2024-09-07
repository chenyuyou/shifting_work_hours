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
4. 裁切数据。根据需要裁切上述下载到的气候数据。本例中是中国中国经纬度包含的矩形面积的数据。china_bounds_file.json是经纬度信息。extract.py是裁切代码。正常情况，裁切速度受到硬盘IO速度和CPU线程数影响，调节代码中works=8，batchsize=2，对于本人机器速度最快。可以写代码测试12个文件，可知最优数。如果换成固态硬盘，可能硬盘IO不是关键因素，CPU线程数影响速度。尽量用固态速度会快很多。裁切数据可以单独设置位置。如果剪切出错，一般都是下载的文件出错了，重新下载这些出错文件，重新运行extract.py，它会自动剪切那些出错的文件，前提是不删除extract.py生成的记录文件processing_status.json。裁切输出到china_output。数据来源为步骤2下载的位置downloaded_data。
5. 计算室内wbgt。直接运行wbgt_indoor_cuda.py即可。运行完成后会出现wbgt_indoor_processing_status.json记录处理进度，如果中断，根据记录恢复，而不必重新算已经算过的，只计算出错和为计算过的。cuda加速效果不明显，估计快几分钟而已。运算数据保存到wbgt_indoor_output文件夹。输入文件为步骤4的输出，即china_output。
6. 计算室外wbgt。直接运行wbgt_outdoor_modified_c.py，它会调用liljegren_cuda_vectorized_c.py。liljegren_cuda_vectorized_c.py文件中计算室外wbgt的方法采用的是https://github.com/mdljts/wbgt/blob/master/src/wbgt.c。R语言计算包计算的温度不收敛，所以没采纳。输入文件为步骤4的输出，即china_output。代码计算结果输出为wbgt_outdoor_output。强调的是，代码必须用cuda加速，否则计算非常非常消耗时间。
7. 计算网格中不同强度人口强度下，劳动生产率损失。实际上就是3种人口强度下，计算了劳动生产率损失，然后乘以人口，得到网格中的劳动生产率损失。需要建立一个文件夹model_outputs，将室内和室外wbgt计算结果wbgt_outdoor_output和wbgt_indoor_output文件夹以及其中文件移动到model_outputs文件夹中。同时找到索要计算区域的人口的nc数据。先将人口数据的坐标与之前计算得到的wbgt数据求交集，然后求积，输出到eighted_productivity_loss_outpu文件夹中。Labor_Productivity_Loss_pop_mini_cuda.py也通过cuda加速。
8. 计算不同劳动强度，所有模型中，不同情景的平均、最大、最小人均劳动力损失。运行labor-productivity-analysis-logical-review.py即可。先用中国地图的geojson数据以及中国分省份geojson数据mask步骤7得到的数据，得到中国以及各省份数据加总，除以人口中国加总和分省份加总。得到人均损失。再计算所有模型，不同情景的平均、最大和最小损失。这里还有个步骤就是考虑了工作时间调解前后这些数据，以及差值。结果保存到labor_productivity_results文件夹中，以csv格式保存。geojson数据阿里云数据可视化平台获取https://datav.aliyun.com/portal/school/atlas/area_selector#&lat=33.54139466898275&lng=104.2822265625&zoom=4