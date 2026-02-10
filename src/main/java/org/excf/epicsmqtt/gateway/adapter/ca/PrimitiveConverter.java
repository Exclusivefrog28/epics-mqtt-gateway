package org.excf.epicsmqtt.gateway.adapter.ca;

public class PrimitiveConverter {

    public static Object toPrimitiveArray(Object[] input) {

        Object output;

        switch (input) {
            case Integer[] ig -> output = new int[ig.length];
            case Long[] lo -> output = new long[lo.length];
            case Double[] du -> output = new double[du.length];
            case Byte[] by -> output = new byte[by.length];
            case Short[] sh -> output = new short[sh.length];
            case Float[] fl -> output = new float[fl.length];
            case Object[] o -> output = new Object[o.length];
        }

        return toPrimitiveArray(input, output);
    }

    public static Object toPrimitiveArray(Object input, Object output) {

        switch (input) {
            case int[] ig -> System.arraycopy(ig, 0, ((int[]) output), 0, ig.length);
            case double[] du -> System.arraycopy(du, 0, ((double[]) output), 0, du.length);
            case byte[] by -> System.arraycopy(by, 0, ((byte[]) output), 0, by.length);
            case short[] sh -> System.arraycopy(sh, 0, ((short[]) output), 0, sh.length);
            case float[] fl -> System.arraycopy(fl, 0, ((float[]) output), 0, fl.length);
            case Object[] o -> System.arraycopy(o, 0, ((Object[]) output), 0, o.length);
            default -> throw new IllegalStateException("Unexpected value: " + input);
        }

        return output;
    }
}
