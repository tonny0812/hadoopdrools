package com.keepgulp.hadoopdrools.mr;

import java.io.IOException;
import java.util.Collection;

import com.keepgulp.hadoopdrools.model.Entry;
import com.keepgulp.hadoopdrools.model.Transaction;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.drools.core.ClassObjectFilter;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.FactHandle;

public class HadoopDroolsMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

    private static KieContainer kieContainer = KieServices.Factory.get().newKieClasspathContainer();

    @Override
    public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {
        KieSession kieSession = kieContainer.getKieBase().newKieSession();

        //Input is a transaction
        Transaction transaction = new Transaction(value.toString());
        kieSession.insert(transaction);


        //Fire mapper-rules
        kieSession.getAgenda().getAgendaGroup("mapper-rules").setFocus();
        kieSession.fireAllRules();
        Collection<FactHandle> entries = kieSession.getFactHandles(new ClassObjectFilter(Entry.class));

        //Get all entries created as outputs
        for (FactHandle handle : entries) {
            Entry entry = (Entry) kieSession.getObject(handle);
            System.out.println(entry);
            output.write(new Text(entry.key), new DoubleWritable(entry.value));
        }

        kieSession.dispose();
    }

}
