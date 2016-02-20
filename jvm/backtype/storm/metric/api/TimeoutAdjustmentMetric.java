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
import java.util.Arrays;

public class TimeoutAdjustmentMetric implements IMetric {
    boolean enable = false;

    int fail_num = 0;
    int total_num = 0;
    int timeout_value = 0; // in millisec
    int default_timeout_ = 0; // in millisec
    int[] time_ptrs;
    int index = 0;

    public TimeoutAdjustmentMetric(int default_timeout) {
        default_timeout_ = default_timeout * 1000;
        timeout_value = default_timeout * 1000;
        time_ptrs = new int[1000];
    }

    public void setEnable() {
        enable = true;
    }
    
    public boolean UpAck(int time_delta) {
        if (enable) {
            time_ptrs[index++] = time_delta;
            if (++total_num >= 1000) {
                reCalculateTimeout();
                return true;
            } else {
                return false;
            }
        }
        return false;
    }

    public boolean UpFail(int time_delta) {
        if (enable) {
        }
        return false;
    }

    public int getTimeoutValue() {
        return timeout_value;
    }

    public void reCalculateTimeout() {
        total_num = 0;
        index = 0;
        Arrays.sort(time_ptrs);
        int ninth = AverageTime(896,905);
        int ninth_five = AverageTime(946,955);
        int ninth_nine = AverageTime(986,995);
        
        if (2 * ninth < ninth_nine) {
            timeout_value = ninth;
        } else if (2 * ninth_five < time_ptrs[999]){
            timeout_value = ninth_five;
        } else {
            timeout_value = time_ptrs[999];
        }
    }

    private int AverageTime(int start, int end) {
        int sum = 0;
        int num = end - start + 1;
        for(int i=start; i<=end; i++) {
            sum += time_ptrs[i];
        }
        return sum/num;
    }
    
    public Object getValueAndReset() {
        return null;
    }
}