#cd ..
#./compile_dataload.sh
cd ../build
#./LoadYCSB configs/load_pages_100m.cfg
#nohup ./LoadYCSB configs/load_data_10g.cfg > loadobject10g.out 2>&1 &
# nohup ./LoadYCSB configs/load_data_100m_r1024.cfg > load100mr1024.out 2>&1 &
#nohup ./LoadYCSB configs/load_data_100m_negative_r1024.cfg > load100mr1024negative.out 2>&1 &
# nohup ./LoadYCSB configs/load_data_100m_negative_r1024_phantom.cfg > load100mr1024negativephantom.out 2>&1 &
#nohup ./LoadYCSB configs/load_data_1g_negative_domain_20g.cfg > load_data_1g_negative_domain_20g.out 2>&1 &
#nohup ./LoadYCSB configs/load_data_1g_negative_domain_20g.cfg > load_data_1g_negative_domain_20g.out 2>&1 &
nohup ./LoadYCSB configs/load_pages_100m_negative_domain_400m.cfg > /home/hippo/Nemo/experiments/loaddata_logs/load_pages_100m_negative_domain_400m.out 2>&1 &
#nohup ./LoadYCSB configs/load_pages_100m_negative_domain_200m.cfg > /home/hippo/Nemo/experiments/loaddata_logs/load_pages_100m_negative_domain_200m.out 2>&1 &
#nohup ./LoadYCSB configs/load_data_100m_negative_large_domain_r1024.cfg > load100mr1024negative_largedomain.out 2>&1 &
