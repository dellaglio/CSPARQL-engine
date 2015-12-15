package eu.larkc.csparql.sparql.jena.service;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.BindingFactory;
import com.hp.hpl.jena.sparql.engine.binding.BindingMap;

public class CacheAcquaTest {
	
	@Test public void shouldInit(){
		Set<Var> keyVars = new HashSet<Var>(Arrays.asList(
				Var.alloc("k1"),
				Var.alloc("k2")
				));
		Set<Var> valueVars = new HashSet<Var>(Arrays.asList(
				Var.alloc("v1"),
				Var.alloc("v2"),
				Var.alloc("v3")
				));
		
		CacheAcqua cache = new CacheAcqua(10, keyVars, valueVars);
		assertEquals(keyVars, cache.getKeyVars());
		assertEquals(valueVars, cache.getValueVars());
	}
	@Test public void shouldGetKeyBinding(){
		//initializing cache
		Set<Var> keyVars = new HashSet<Var>(Arrays.asList(
				Var.alloc("k1"),
				Var.alloc("k2")
				));
		Set<Var> valueVars = new HashSet<Var>(Arrays.asList(
				Var.alloc("v1"),
				Var.alloc("v2"),
				Var.alloc("v3")
				));
		
		CacheAcqua cache = new CacheAcqua(10, keyVars, valueVars);
		//defining a binding 
		BindingMap bm = BindingFactory.create();
		bm.add(Var.alloc("k1"), NodeFactory.createLiteral("a"));
		bm.add(Var.alloc("k2"), NodeFactory.createLiteral("b"));
		bm.add(Var.alloc("v1"), NodeFactory.createLiteral("c"));
		bm.add(Var.alloc("v2"), NodeFactory.createLiteral("d") );
		bm.add(Var.alloc("v3"), NodeFactory.createLiteral("e") );
		//defining the keybinding
		BindingMap kbm = BindingFactory.create();
		kbm.add(Var.alloc("k1"), NodeFactory.createLiteral("a"));
		kbm.add(Var.alloc("k2"), NodeFactory.createLiteral("b"));
		//get the value of keybining
		assertEquals(kbm, cache.getKeyBinding(bm));
		
	}
	@Test public void testValueBinding(){
		//initializing cache
		Set<Var> keyVars = new HashSet<Var>(Arrays.asList(
				Var.alloc("k1"),
				Var.alloc("k2")
				));
		Set<Var> valueVars = new HashSet<Var>(Arrays.asList(
				Var.alloc("v1"),
				Var.alloc("v2"),
				Var.alloc("v3")
				));
		
		CacheAcqua cache = new CacheAcqua(10, keyVars, valueVars);
		//defining a binding 
		BindingMap bm = BindingFactory.create();
		bm.add(Var.alloc("k1"), NodeFactory.createLiteral("a"));
		bm.add(Var.alloc("k2"), NodeFactory.createLiteral("b"));
		bm.add(Var.alloc("v1"), NodeFactory.createLiteral("c"));
		bm.add(Var.alloc("v2"), NodeFactory.createLiteral("d") );
		bm.add(Var.alloc("v3"), NodeFactory.createLiteral("e") );
		//defining the keybinding
		BindingMap vbm = BindingFactory.create();
		vbm.add(Var.alloc("v1"), NodeFactory.createLiteral("c"));
		vbm.add(Var.alloc("v2"), NodeFactory.createLiteral("d") );
		vbm.add(Var.alloc("v3"), NodeFactory.createLiteral("e") );
		//get the value of keybining
		assertEquals(vbm, cache.getValueBinding(bm));
		
	}
	@Test public void shouldAddElements(){
		BindingMap bm = BindingFactory.create();
		bm.add(Var.alloc("k1"), NodeFactory.createLiteral("a"));
		bm.add(Var.alloc("k2"), NodeFactory.createLiteral("b"));
		bm.add(Var.alloc("v1"), NodeFactory.createLiteral("c"));
		bm.add(Var.alloc("v2"), NodeFactory.createLiteral("d") );
		bm.add(Var.alloc("v3"), NodeFactory.createLiteral("e") );
		
		BindingMap bmKey = BindingFactory.create();
		bmKey.add(Var.alloc("k1"), NodeFactory.createLiteral("a"));
		bmKey.add(Var.alloc("k2"), NodeFactory.createLiteral("b"));


		Set<Var> keyVars = new HashSet<Var>(Arrays.asList(
				Var.alloc("k1"),
				Var.alloc("k2")
				));
		Set<Var> valueVars = new HashSet<Var>(Arrays.asList(
				Var.alloc("v1"),
				Var.alloc("v2"),
				Var.alloc("v3")
				));
		
		CacheAcqua cache = new CacheAcqua(10, keyVars, valueVars);
		assertEquals(false, cache.contains(bmKey));
		cache.put(bm);
		assertEquals(true, cache.contains(bmKey));
}
}
