package simpledb.optimizer;

import simpledb.execution.Predicate;

/** 表示单个基于整数的字段上的固定宽度直方图的类。
 */
public class IntHistogram {
    private int[] buckets;  //直方图中的每个条形

    private int min;

    private int max;

    private double width;   //每个桶的宽度

    private int tuplesCount;    //整个直方图统计的元组数

    /**
     * 创建一个新的 IntHistogram。
     *
     * 他的 IntHistogram 应该维护它接收的整数值的直方图。它应该将直方图拆分为“桶”桶。
     *
     * 直方图中的值将通过“addValue()”函数一次提供一个。
     *
     * 您的实现应该使用空间并具有相对于直方图值的数量恒定的执行时间。
     * 只需将您看到的每个值都存储在排序列表中。
     *
     * @param buckets 要将输入值拆分成的桶数
     * @param min 将传递给此类以进行直方图的最小整数值
     * @param max 将传递给此类进行直方图的最大整数值
     */
    public IntHistogram(int buckets, int min, int max) {
        this.buckets = new int[buckets];
        this.min = min;
        this.max = max;
        this.width = (max - min + 1.0) / buckets;
        this.tuplesCount = 0;
    }

    /**
     * 将一个值添加到您要保留其直方图的值集。
     * @param v 添加到直方图中的值
     */
    public void addValue(int v) {
        if(v >= min && v <= max){
            int index = getIndex(v);
            buckets[index]++;
            tuplesCount++;
        }
    }

    /**
     * 根据value获得桶的序号
     * @param v
     * @return
     */
    private int getIndex(int v){
        return (int) ((v - min) / width);
    }

    /**
     * 估计此表上特定谓词和操作数的选择性。
     *
     * 例如，如果“op”为“GREATER_THAN”且“v”为 5，则返回对大于 5 的元素比例的估计。
     *
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        double selectivity = 0;
        switch (op) {
            case GREATER_THAN:
                selectivity = estimateSelectivity_greater_than(v);
                break;
            case EQUALS:
                selectivity = estimateSelectivity_equal(v);
                break;
            case LESS_THAN:
                selectivity = 1 - estimateSelectivity_equal(v) - estimateSelectivity_greater_than(v);
                break;
            case LESS_THAN_OR_EQ:
                selectivity = 1 - estimateSelectivity_greater_than(v);
                break;
            case GREATER_THAN_OR_EQ:
                selectivity = estimateSelectivity_equal(v) + estimateSelectivity_greater_than(v);
                break;
            case NOT_EQUALS:
                selectivity = 1 - estimateSelectivity_equal(v);
                break;
            default:
                return -1;
        }
        return selectivity;
    }

    private double estimateSelectivity_equal(int v) {
        if (tuplesCount == 0) {
            return 0.0;
        }
        if (v < min || v > max) {
            return 0;
        }
        int index = getIndex(v);
        int height = buckets[index];
        return width > 1 ? height / width / tuplesCount : height * 1.0 / tuplesCount;
    }

    private double estimateSelectivity_greater_than(int v) {
        double total = 0;
        if (v > max) {
            return 0;
        }
        if (v < min) {
            return 1.0;
        }
        int index = getIndex(v);
        int height = buckets[index];
        double avg = height / width;
        // 1. count bucket greater then current bucket
        for (int i = index + 1; i < buckets.length;i++) {
            total += buckets[i];
        }
        // 2. count greater than v in current bucket [ )
        if (width > 1) {
            double left_bucket = index * width + min;
            double left_width = v - left_bucket + 1;
            total += (height - (left_width) * avg);
            return total / tuplesCount;
        } else {
            return total / tuplesCount;
        }
    }

    /**
     * @return
     *     此直方图的平均选择性。
     *     这不是实现基本连接优化的必不可少的方法。如果您想实现更有效的优化，可能需要它
     * */
    public double avgSelectivity() {
        return tuplesCount / tuplesCount;
    }

    /**
     * @return 描述此直方图的字符串，用于调试目的
     */
    public String toString() {
        return String.format("IntHistgram(buckets=%d, min=%d, max=%d",
                buckets.length, min, max);
    }
}

