/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm.metric.api;

import backtype.storm.metric.api.IMetric;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Collections;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import backtype.storm.generated.Grouping;
import java.lang.Math;
import java.util.Comparator;


public class IShuffleGAdjustmentMetric implements IMetric {
    public static final Logger LOG = LoggerFactory.getLogger(IShuffleGAdjustmentMetric.class);
    public HashMap<String, HashMap<String, HashMap<Integer, List>>> downstream_tasks_;
    private Random rand;

    public IShuffleGAdjustmentMetric(Map<String, Map<String, Grouping>> targets, Map<String, List<Integer>> componentTosortedTasks) {
        rand = new Random(Utils.secureRandomLong());
        downstream_tasks_ = new HashMap<String, HashMap<String, HashMap<Integer, List>>>();

        //Initialize downstream_tasks_
        for(Map.Entry<String, Map<String, Grouping>> s_bucket : targets.entrySet()) {
            String stream_id = s_bucket.getKey();
            for(Map.Entry<String, Grouping> c_bucket : s_bucket.getValue().entrySet()) {
                if(c_bucket.getValue().is_set_shuffle()) {

                    String component_id = c_bucket.getKey();
                    HashMap<String, HashMap<Integer, List>> s_bck = downstream_tasks_.get(stream_id);
                    if(s_bck == null) {
                        s_bck = new HashMap<String, HashMap<Integer, List>>();
                        downstream_tasks_.put(stream_id, s_bck);
                    }

                    HashMap<Integer, List> c_bck = new HashMap<Integer, List>();
                    s_bck.put(component_id, c_bck);

                    List<Integer> ts = componentTosortedTasks.get(component_id);
                    if(ts == null) {
                        LOG.info("Error when construct a IShuffleGAdjustmentMetric object.");
                    } else {
                        int base_amount = 100/ts.size();
                        int mod = 100 % ts.size();
                        int index = 0;
                        for(Integer task_id : ts) {
                            ArrayList tasks_info = new ArrayList();
                            tasks_info.add(new Double(1));
                            tasks_info.add(new Double(1));
                            tasks_info.add(new Double(1)); // store aging latency
                            // store amount of traffic it get, useful in AIAD
                            if (index++ < mod) {
                                tasks_info.add(new Integer(base_amount + 1));
                            } else {
                                tasks_info.add(new Integer(base_amount));
                            }
                            c_bck.put(task_id, tasks_info);
                        }
                    }
                }
            }
        }
    }


    public void adjustDownstreamRatio(HashMap<String, HashMap<String, ArrayList<Integer>>> ret, int mode) {
        if (mode == 0) {
            // do nothing
        } else if (mode == 1) {
            AIAD(ret);
        } else if (mode == 2) {
            PowerOfTwoChoice(ret);
        } else {

        }
    }

    static public void AgingProcess(List tasks_info, double aging_rate) {
        tasks_info.set(2, (Double)tasks_info.get(2)*(1-aging_rate)+(Double)tasks_info.get(0)*aging_rate);
    }

    private class Latency_info {
        public Double latency;
        public Integer task_id;

        public Latency_info(Double l, Integer t_id) {
            latency = l;
            task_id = t_id;
        }
    }

    private class LatencyComparator implements Comparator<Latency_info>{
        public int compare(Latency_info a, Latency_info b) {
            if (a.latency < b.latency) {
                return -1;
            } else if (a.latency > b.latency) {
                return 1;
            } else {
                return 0;
            }
        }
    }


    private void AIAD(HashMap<String, HashMap<String, ArrayList<Integer>>> ret) {

        for (Map.Entry<String, HashMap<String, HashMap<Integer, List>>> s_bucket : downstream_tasks_.entrySet()) {
            String stream_id = s_bucket.getKey();
            for (Map.Entry<String, HashMap<Integer, List>> c_bucket : s_bucket.getValue().entrySet()) {
                String component_id = c_bucket.getKey();

                ArrayList<Integer> tmp;
                if (ret.get(stream_id) != null && ret.get(stream_id).get(component_id)!= null) {
                    tmp = ret.get(stream_id).get(component_id);
                } else {
                    LOG.info("Error when preform adjustDownstreamRatio method.");
                    continue;
                }

                while(tmp.size() < 100) {
                    tmp.add(null);
                }

                HashMap<Integer, List> tasks_info = c_bucket.getValue();

                ArrayList<Latency_info> latency_info = new ArrayList<Latency_info>();
                for (Map.Entry<Integer, List> task_info : tasks_info.entrySet()) {
                    latency_info.add(new Latency_info((Double)task_info.getValue().get(2), task_info.getKey()));
                }
                Collections.sort(latency_info, new LatencyComparator());
                
                int size = latency_info.size();
                for(int i=0; i<size/2; i++) {
                    if (latency_info.get(i).latency * 1.2 < latency_info.get(size-1-i).latency) {
                        if ((Integer)tasks_info.get(latency_info.get(size-1-i).task_id).get(3) > 1) {
                            tasks_info.get(latency_info.get(size-1-i).task_id).set(3, (Integer)tasks_info.get(latency_info.get(size-1-i).task_id).get(3)-1);
                            tasks_info.get(latency_info.get(i).task_id).set(3, (Integer)tasks_info.get(latency_info.get(i).task_id).get(3)+1);
                        }
                    } else {
                        break;
                    }
                }

                int index = 0;
                for (Map.Entry<Integer, List> task_info : tasks_info.entrySet()) {
                    for(int i=0; i<(Integer)task_info.getValue().get(3); i++) {
                        tmp.set(index++, task_info.getKey());
                    }
                }

                Collections.shuffle(tmp, rand);   
            }
        }
    }

    
    public void PowerOfTwoChoice(HashMap<String, HashMap<String, ArrayList<Integer>>> ret) {

        for (Map.Entry<String, HashMap<String, HashMap<Integer, List>>> s_bucket : downstream_tasks_.entrySet()) {
            String stream_id = s_bucket.getKey();
            for (Map.Entry<String, HashMap<Integer, List>> c_bucket : s_bucket.getValue().entrySet()) {
                String component_id = c_bucket.getKey();

                ArrayList<Integer> tmp;
                if (ret.get(stream_id) != null && ret.get(stream_id).get(component_id)!= null) {
                    tmp = ret.get(stream_id).get(component_id);
                } else {
                    LOG.info("Error when preform adjustDownstreamRatio method.");
                    continue;
                }

                while(tmp.size() < 100) {
                    tmp.add(null);
                }

                ArrayList<Integer> choice1 = new ArrayList<Integer>();
                ArrayList<Integer> choice2 = new ArrayList<Integer>();

                HashMap<Integer, List> tasks_info = c_bucket.getValue();

                for (Map.Entry<Integer, List> entry : tasks_info.entrySet()) {
                    for(int i=0; i<(Integer)entry.getValue().get(3); i++) {
                        choice1.add(entry.getKey());
                        choice2.add(entry.getKey());
                    }
                }

                Collections.shuffle(choice1, rand);
                Collections.shuffle(choice2, rand);


                for(int j=0; j<choice1.size(); j++) {
                    if((Double)tasks_info.get(choice1.get(j)).get(2) <= (Double)tasks_info.get(choice2.get(j)).get(2)) {
                        tmp.set(j, choice1.get(j));
                    } else {
                        tmp.set(j, choice2.get(j));
                    }
                }
            }
        }
    }
    
    /*
    public void adjustDownstreamRatio(HashMap<String, HashMap<String, ArrayList<Integer>>> ret) {

        for (Map.Entry<String, HashMap<String, HashMap<Integer, List<Double>>>> s_bucket : downstream_tasks_.entrySet()) {
            String stream_id = s_bucket.getKey();
            for (Map.Entry<String, HashMap<Integer, List<Double>>> c_bucket : s_bucket.getValue().entrySet()) {
                String component_id = c_bucket.getKey();

                ArrayList<Integer> tmp;
                if (ret.get(stream_id) != null && ret.get(stream_id).get(component_id)!= null) {
                    tmp = ret.get(stream_id).get(component_id);
                } else {
                    LOG.info("Error when preform adjustDownstreamRatio method.");
                    continue;
                }

                while(tmp.size() < 100) {
                    tmp.add(null);
                }

                double total = 0;
                double traffic_sum = 0;
                double task_cnt = c_bucket.getValue().size();
                double average_latency = 0;

                for (Map.Entry<Integer, List<Double>> entry : c_bucket.getValue().entrySet()) {
                    average_latency += entry.getValue().get(0);
                    traffic_sum += entry.getValue().get(1);
                }

                average_latency = average_latency/c_bucket.getValue().size();
                double average_traffic = traffic_sum/task_cnt;

                for (Map.Entry<Integer, List<Double>> entry : c_bucket.getValue().entrySet()) {
                   List<Double> curr = entry.getValue();
                   double term = 1;
                   if((curr.get(1)<average_traffic && curr.get(0)<average_latency) || (curr.get(1)>average_traffic && curr.get(0)>average_latency)) {
                        term = Math.log1p(average_traffic/curr.get(1)) / Math.log(2);
                   }
                   curr.set(0, curr.get(1) * term * Math.log1p(1/curr.get(0)));
                   

                   total += curr.get(0);
                }

                int idx_ = 0;
                List<Integer> minor = new ArrayList<Integer>();
                for (Map.Entry<Integer, List<Double>> entry : c_bucket.getValue().entrySet()) {
                    int ratio = (int)(100 * entry.getValue().get(0) / total);
                    Integer key = entry.getKey();
                    
                    if(ratio == 0) {
                        minor.add(key);
                    } else {
                        for(int i=0; i<ratio; i++) {
                            tmp.set(idx_++, key);
                        }
                    }
                }

                int s_size = minor.size();
                if (s_size == 0) {
                    for (Map.Entry<Integer, List<Double>> entry : c_bucket.getValue().entrySet()) {
                        Integer key = entry.getKey();
                        if(idx_<100) {
                            tmp.set(idx_++, key);
                        } else {
                            break;
                        }
                    }
                } else {
                    int i = 0;
                    while(idx_ < 100) {
                        tmp.set(idx_++, minor.get(i++));
                        i = i%s_size;
                    }
                }

                Collections.shuffle(tmp, rand);
            }
        }
    }*/
    

    public Object getValueAndReset() {
        return null;
    }
}
