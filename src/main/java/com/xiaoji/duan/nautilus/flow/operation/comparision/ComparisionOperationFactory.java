package com.xiaoji.duan.nautilus.flow.operation.comparision;

public class ComparisionOperationFactory {

	public static ComparisionOperation createByName(String op) throws ClassNotFoundException, InstantiationException, IllegalAccessException {

		if (op.startsWith("$")) {
			Class clazz = Class.forName("com.xiaoji.duan.nautilus.flow.operation.comparision." + (new StringBuilder()).append(Character.toUpperCase(op.charAt(1))).append(op.toLowerCase().substring(2)));
			
			return (ComparisionOperation) clazz.newInstance();
		} else {
			Class clazz = Class.forName("com.xiaoji.duan.nautilus.flow.operation.comparision." + (new StringBuilder()).append(Character.toUpperCase(op.charAt(0))).append(op.toLowerCase().substring(1)));
			
			return (ComparisionOperation) clazz.newInstance();
		}
	}
}
