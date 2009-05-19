package com.othersonline.kv.annotations;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import com.sun.mirror.apt.AnnotationProcessor;
import com.sun.mirror.apt.AnnotationProcessorEnvironment;
import com.sun.mirror.apt.AnnotationProcessorFactory;
import com.sun.mirror.apt.AnnotationProcessors;
import com.sun.mirror.declaration.AnnotationTypeDeclaration;

public class RegisterProcessorFactory implements AnnotationProcessorFactory {

	public AnnotationProcessor getProcessorFor(
			Set<AnnotationTypeDeclaration> declarations,
			AnnotationProcessorEnvironment env) {
		AnnotationProcessor result;
		if (declarations.isEmpty()) {
			result = AnnotationProcessors.NO_OP;
		} else {
			result = new RegisterAnnotationProcessor(env);
		}
		return result;
	}

	public Collection<String> supportedAnnotationTypes() {
		return Collections.singletonList(Register.class.getName());
	}

	public Collection<String> supportedOptions() {
		return Collections.emptyList();
	}

}
