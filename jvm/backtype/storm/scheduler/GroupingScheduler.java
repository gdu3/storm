package backtype.storm.scheduler;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
 
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
 
import backtype.storm.generated.Bolt;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
 
public class GroupingScheduler implements IScheduler{
 
    @Override
    public void prepare(Map conf) {
         
    }
 
    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        
        Collection<TopologyDetails> topologyDetails = topologies.getTopologies();
        Collection<SupervisorDetails> supervisorDetails = cluster.getSupervisors().values();
        Map<String, SupervisorDetails> supervisors = new HashMap<String, SupervisorDetails>();
        
        for(SupervisorDetails s : supervisorDetails){
            Map<String, Object> metadata = (Map<String, Object>)s.getSchedulerMeta();
            if(metadata.get("groupId") != null){
                supervisors.put((String)metadata.get("groupId"), s);
            }
        }

        Object [] supervisorDetailsArray = supervisorDetails.toArray();
        int cluster_size = supervisorDetailsArray.length;
        Map<SupervisorDetails, List<ExecutorDetails>> assignments = new HashMap<SupervisorDetails, List<ExecutorDetails>>();
        for (SupervisorDetails s: supervisorDetails) {
            assignments.put(s, new ArrayList<ExecutorDetails>());
        }

         
        for(TopologyDetails t : topologyDetails){

            StormTopology topology = t.getTopology();
            Map<String, Bolt> bolts = topology.get_bolts();
            Map<String, SpoutSpec> spouts = topology.get_spouts();
            JSONParser parser = new JSONParser();
            int cnt = 0;

            try {
                Map<String, List<ExecutorDetails>> components = cluster.getNeedsSchedulingComponentToExecutors(t);
                for (Map.Entry<String, List<ExecutorDetails>> entry : components.entrySet()) {
                    String name = entry.getKey();
                    List<ExecutorDetails> list_e = entry.getValue();
                    
                    if (bolts.get(name) != null) {
                        Bolt bolt = bolts.get(name);
                        JSONObject conf = (JSONObject)parser.parse(bolt.get_common().get_json_conf());
                        if(conf.get("groupId") != null && supervisors.get(conf.get("groupId")) != null){
                            List<ExecutorDetails> assignment = assignments.get(supervisors.get(conf.get("groupId")));
                            assignment.addAll(list_e);

                        } else {
                            for (ExecutorDetails e : list_e) {
                                List<ExecutorDetails> assignment = assignments.get(supervisorDetailsArray[cnt % cluster_size]);
                                assignment.add(e);
                                cnt++;
                            }
                        }
                    } else {
                        for (ExecutorDetails e : list_e) {
                            List<ExecutorDetails> assignment = assignments.get(supervisorDetailsArray[cnt % cluster_size]);
                            assignment.add(e);
                            cnt++;
                        }
                    }

                }

            } catch(ParseException pe){
                pe.printStackTrace();
            }

            

            for (Map.Entry<SupervisorDetails, List<ExecutorDetails>> entry : assignments.entrySet()) {
                SupervisorDetails supervisor = entry.getKey();
                List<WorkerSlot> availableSlots = cluster.getAvailableSlots(supervisor);
                List<ExecutorDetails> executors = entry.getValue();

                if(!availableSlots.isEmpty() && executors.size() != 0){
                    cluster.assign(availableSlots.get(0), t.getId(), executors);
                }
            }

        }
    }
     
}