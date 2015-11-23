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


public class IShuffleGAdjustmentMetric implements IMetric {
    public static final Logger LOG = LoggerFactory.getLogger(IShuffleGAdjustmentMetric.class);
    public HashMap<String, HashMap<String, HashMap<Integer, List<Double>>>> downstream_tasks_;
    private Random rand;

    public IShuffleGAdjustmentMetric(Map<String, Map<String, Grouping>> targets, Map<String, List<Integer>> componentTosortedTasks) {
        rand = new Random(Utils.secureRandomLong());
        downstream_tasks_ = new HashMap<String, HashMap<String, HashMap<Integer, List<Double>>>>();

        //Initialize downstream_tasks_
        for(Map.Entry<String, Map<String, Grouping>> s_bucket : targets.entrySet()) {
            String stream_id = s_bucket.getKey();
            for(Map.Entry<String, Grouping> c_bucket : s_bucket.getValue().entrySet()) {
                if(c_bucket.getValue().is_set_shuffle()) {

                    String component_id = c_bucket.getKey();
                    HashMap<String, HashMap<Integer, List<Double>>> s_bck = downstream_tasks_.get(stream_id);
                    if(s_bck == null) {
                        s_bck = new HashMap<String, HashMap<Integer, List<Double>>>();
                        downstream_tasks_.put(stream_id, s_bck);
                    }

                    HashMap<Integer, List<Double>> c_bck = new HashMap<Integer, List<Double>>();
                    s_bck.put(component_id, c_bck);

                    List<Integer> ts = componentTosortedTasks.get(component_id);
                    if(ts == null) {
                        LOG.info("Error when construct a IShuffleGAdjustmentMetric object.");
                    } else {
                        for(Integer task_id : ts) {
                            ArrayList<Double> task_info = new ArrayList<Double>();
                            task_info.add(new Double(1));
                            task_info.add(new Double(1));
                            c_bck.put(task_id, task_info);
                        }
                    }
                }
            }
        }
    }

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

                double total = 0;
                for (Map.Entry<Integer, List<Double>> entry : c_bucket.getValue().entrySet()) {
                    total += entry.getValue().get(1) / entry.getValue().get(0);
                }

                while(tmp.size() < 100) {
                    tmp.add(null);
                }

                int idx_ = 0;
                for (Map.Entry<Integer, List<Double>> entry : c_bucket.getValue().entrySet()) {
                    int ratio = (int)(100 * (entry.getValue().get(1)/entry.getValue().get(0)) / total);
                    Integer key = entry.getKey();
                    for(int i=0; i<ratio; i++) {
                        tmp.set(idx_++, key);
                    }
                }
                Integer last_key = tmp.get(idx_ - 1);
                while(idx_ < 100) {
                    tmp.set(idx_++, last_key);
                }
                Collections.shuffle(tmp, rand);
            }
        }
    }

    public Object getValueAndReset() {
        return null;
    }
}
