package com.keepgulp.hadoopdrools.mr;

import java.io.IOException;

import com.keepgulp.hadoopdrools.model.Entry;
import com.keepgulp.hadoopdrools.model.RunningValue;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;

public class HadoopDroolsReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

    private static KieContainer kieContainer = KieServices.Factory.get().newKieClasspathContainer();

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> textValues, Context output)
            throws IOException, InterruptedException {

        if (!textValues.iterator().hasNext()) {
            // No values!
            return;
        }

        RunningValue runningReduce = null;

        for (DoubleWritable nextValue : textValues) {
            if (runningReduce == null) {
                runningReduce = new RunningValue(key.toString(),
                        nextValue.get());
            } else {
                KieSession kieSession = kieContainer.getKieBase().newKieSession();
                kieSession.insert(runningReduce);
                Entry entry = new Entry(key.toString(), nextValue.get());
                kieSession.insert(entry);
                kieSession.getAgenda().getAgendaGroup("reducer-rules").setFocus();
                kieSession.fireAllRules();
                kieSession.dispose();
            }
        }
        output.write(new Text(runningReduce.key), new DoubleWritable(runningReduce.value));
    }
}
