# shifting_work_hours

1. 获取带下载的文件信息。nasa_climate_data是爬虫工具scrapy写的脚本，是用来从nasa下载气候有关数据信息的。运行后得到的nasa_climate_data_info.csv记录了准备下载的具体数据信息。
2. 下载文件。下载过程涉及两个python代码，分别是climate_data_downloader.py和rebuild_metadata.py。以及一个window下bat脚本auto_restart.bat。如果设置正确，双击auto_restart.bat是能够直接下载，该bat脚本会运行climate_data_downloader.py进行下载，并且如果下载中断，会自动重新继续下载。主要设置是确定conda的位置。rebuild_metadata.py脚本的作用是有可能下载中断，重建下载历史信息，确保不会重复下载。