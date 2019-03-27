import java.lang.instrument.ClassFileTransformer;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.ClassClassPath;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.NotFoundException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;

public class SchedulerTransformer implements ClassFileTransformer {
    static Logger log = LoggerFactory.getLogger(SchedulerTransformer.class);

    private String targetClassName;
    private ClassLoader targetClassLoader;

    static String SUBMIT_STAGE = "org$apache$spark$scheduler$DAGScheduler$$submitStage";

    public SchedulerTransformer(String targetClassName,
                               ClassLoader targetClassLoader) {
        this.targetClassName = targetClassName;
        this.targetClassLoader = targetClassLoader;
    }

    @Override
    public byte[] transform(ClassLoader loader,
                            String className,
                            Class<?> target,
                            ProtectionDomain protectionDomain,
                            byte[] classfileBuffer)
        throws IllegalClassFormatException {
        String finalTargetClassName = this.targetClassName
            .replaceAll("\\.", "/");

        if(!className.equals(finalTargetClassName)) {
            return classfileBuffer;
        }
        byte[] byteCode = classfileBuffer;

        if(loader.equals(this.targetClassLoader)) {
            log.info("Transforming class: " + targetClassName);

            try {
                ClassPool cp = ClassPool.getDefault();
                cp.insertClassPath(new ClassClassPath(org.apache.spark.scheduler.DAGScheduler.class));
                CtClass cc = cp.get(this.targetClassName);

                CtMethod targetMethod = cc.getDeclaredMethod(SUBMIT_STAGE);
                StringBuilder toAdd = new StringBuilder();
                toAdd.append("DAGVisualizer.get().clear();");
                toAdd.append("DAGVisualizer.get().submitAll(stage.rdd());\n");
                targetMethod.insertBefore(toAdd.toString());

                byteCode = cc.toBytecode();
                cc.detach();
            } catch (NotFoundException | CannotCompileException | IOException e) {
                log.error("Exception while transforming class", e);
            }
        }

        return byteCode;
    }
}
