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

public class QueueWaitTimeMetric implements IMetric {
    int count = 1;
    double sum = 1.0;
    int round = 0;
    int define_round_;

    public QueueWaitTimeMetric(int define_round) {
        define_round_ = define_round;
    }
    
    // Every 2048 updates, reset.
    public void update(long value) {
        sum += value;
        count++;
        if(++round == define_round_) {
            round = 0;
            getValueAndReset();
        }
    }

    public synchronized Object getValueAndReset() {
        sum = (sum/count);
        count = 1;
        return null;
    }

    public synchronized Double getAverageWaitTime() {
        double ret = sum / count;
        return ret;
    }
}
