import java.lang.instrument.ClassFileTransformer;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.NotFoundException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;

public class ExecutorTransformer implements ClassFileTransformer {
    static Logger log = LoggerFactory.getLogger(ExecutorTransformer.class);

    private String targetClassName;
    private ClassLoader targetClassLoader;

    public ExecutorTransformer(String targetClassName,
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
            log.info("Transforming class Executor");

            try {
                ClassPool cp = ClassPool.getDefault();
                CtClass cc = cp.get(this.targetClassName);

                CtMethod runMethod = cc.getDeclaredMethod("run");
                runMethod.insertBefore("System.out.println(\"ONE OF US\");");

                byteCode = cc.toBytecode();
                cc.detach();
            } catch (NotFoundException | CannotCompileException | IOException e) {
                log.error("Exception while transforming class", e);
            }
        }

        return byteCode;
    }
}
