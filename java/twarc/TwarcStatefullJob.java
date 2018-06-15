package twarc;

import org.quartz.Job;
import org.quartz.PersistJobDataAfterExecution;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.IPersistentMap;
import clojure.lang.ISeq;
import clojure.java.api.Clojure;

@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class TwarcStatefullJob implements Job {
    private IFn f;
    private IPersistentMap scheduler;
    public TwarcStatefullJob(Object f, Object scheduler) {
        this.f = (IFn) f;
        this.scheduler = (IPersistentMap) scheduler;
    }

    public void execute(JobExecutionContext context) {
        JobDataMap m = context.getMergedJobDataMap();
        IFn list = Clojure.var("clojure.core", "list*");
        ISeq args = (ISeq) list.invoke(scheduler.assoc(Keyword.intern("twarc", "execution-context"), context), m.get("state"), m.get("arguments"));
        Object result = f.applyTo(args);
        m.put("state", result);
    }
}
