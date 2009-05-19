package com.othersonline.kv.annotations;

import java.util.Collection;

import com.sun.mirror.apt.AnnotationProcessor;
import com.sun.mirror.apt.AnnotationProcessorEnvironment;
import com.sun.mirror.declaration.AnnotationTypeDeclaration;
import com.sun.mirror.declaration.Declaration;

public class RegisterAnnotationProcessor implements AnnotationProcessor {
	private AnnotationProcessorEnvironment env;

	private AnnotationTypeDeclaration declaration;

	public RegisterAnnotationProcessor(AnnotationProcessorEnvironment env) {
		this.env = env;
		// get the annotation type declaration for our 'Note' annotation.
		// Note, this is also passed in to our annotation factory - this
		// is just an alternate way to do it.
		this.declaration = (AnnotationTypeDeclaration) env
				.getTypeDeclaration(Register.class.getName());
	}

	public void process() {
		System.err.println("process()");
		// Get all declarations that use the note annotation.
		Collection<Declaration> declarations = env
				.getDeclarationsAnnotatedWith(declaration);
		for (Declaration declaration : declarations) {
			processRegisterAnnotation(declaration);
		}

	}

	private void processRegisterAnnotation(Declaration declaration) {

	}
}
