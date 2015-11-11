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

public class TimeoutAdjustmentMetric implements IMetric {
    boolean enable = false;
    //int ack_num = 0;
    int fail_num = 0;
    int total_num = 0;
    double timeout_value = 0; // in millisec
    double default_timeout_ = 0; // in millisec

    public TimeoutAdjustmentMetric(int default_timeout) {
        default_timeout_ = default_timeout * 1000;
        timeout_value = default_timeout * 1000;
    }

    public void setEnable() {
        enable = true;
    }
    
    public boolean UpAck() {
        if (enable) {
            //++ack_num;
            if (++total_num >= 1500) {
                reCalculateTimeout();
                return true;
            } else {
                return false;
            }
        }
        return false;
    }

    public boolean UpFail() {
        if (enable) {
            ++fail_num;
            if (++total_num >= 1500) {
                reCalculateTimeout();
                return true;
            } else {
                return false;
            }
        }
        return false;
    }

    public int getTimeoutValue() {
        return (int)(timeout_value);
    }

    public void reCalculateTimeout() {
        if (fail_num < 15) {
            timeout_value = timeout_value * 0.8;
        } else if (fail_num > 105) {
            timeout_value = min(timeout_value * 2, default_timeout_);
        } else if (fail_num >= 15 && fail_num < 45) {
            timeout_value = timeout_value * 0.9;
        }

        fail_num = 0;
        total_num = 0;
    }

    double min(double a, double b) {
        return a<b? a : b;
    }

    public Object getValueAndReset() {
        return null;
    }
}