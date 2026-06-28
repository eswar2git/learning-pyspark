# Quick Reference Guide — Spark Partitioning & Bucketing

## 🎯 Decision Tree

```
Question: What column are you filtering/joining on?
    ↓
Q1: Does it have < 100 distinct values?
    Yes → Use partitionBy ✅
    No → Go to Q2
    ↓
Q2: Do you frequently join on this column?
    Yes → Use Bucketing ✅
    No → Use repartition() for general parallelism
```

## 📊 Comparison Matrix

| Feature | partitionBy | repartition | coalesce | Bucketing |
|---------|-----------|------------|----------|-----------|
| **Shuffle** | At write time | Full shuffle | No shuffle | At write time |
| **Cardinality** | Low | Any | Any | High |
| **Use Case** | Filtering | Parallelism | Reduce files | Joins |
| **Directory Output** | Yes | No | No | No |
| **Pruning** | Yes | No | No | Partial |

## 🚀 Common Patterns

### Pattern 1: Low-Cardinality Filtering
```python
# Sales data by region/date
df.write.mode('overwrite') \
    .partitionBy('region', 'date') \
    .parquet('sales_data')

# Read only North region
spark.read.parquet('sales_data') \
    .filter(col('region') == 'North')  # Prunes to North/ directory
```

### Pattern 2: High-Cardinality Joins
```python
# Bucket user activity
df_activity.write.mode('overwrite') \
    .bucketBy(64, 'user_id') \
    .sortBy('user_id') \
    .saveAsTable('user_activity')

# Bucket user metadata with same count
df_meta.write.mode('overwrite') \
    .bucketBy(64, 'user_id') \
    .sortBy('user_id') \
    .saveAsTable('user_meta')

# Join — no shuffle needed
df_activity.join(df_meta, 'user_id')
```

### Pattern 3: Optimize Before Writing
```python
# Reduce small files before write
df.coalesce(10) \
    .write.mode('overwrite') \
    .parquet('optimized_output')
```

## ⚙️ Configuration Tuning

```python
# Increase shuffle parallelism for large datasets
spark.conf.set("spark.sql.shuffle.partitions", "256")

# Adjust partition size (default: 128MB)
spark.conf.set("spark.sql.files.maxPartitionBytes", "256m")

# Adjust open cost overhead
spark.conf.set("spark.sql.files.openCostInBytes", "4m")

# Set default parallelism
spark.conf.set("spark.default.parallelism", 64)
```

## 📈 Performance Tips

1. **Profile before optimizing** — use `explain()` to understand current plan
2. **Measure the difference** — time queries before and after changes
3. **Watch for data skew** — use Spark UI to monitor task distribution
4. **Start conservative** — begin with fewer partitions, increase if needed
5. **Document your choices** — explain why you chose a specific strategy

## 🔍 Debug Commands

```python
# Check partition count
df.rdd.getNumPartitions()

# View partition distribution
df.rdd.glom().map(len).collect()

# Detailed query plan
df.explain(mode='extended')

# Check configuration value
spark.conf.get("spark.sql.shuffle.partitions")

# CPU cores available
import os
os.cpu_count()
```

## ❌ Mistakes to Avoid

❌ Partitioning by unique ID column (100K+ partitions)  
✅ Use bucketing instead

❌ Having different bucket counts on join tables  
✅ Align bucket counts for optimal performance

❌ Ignoring partition pruning in queries  
✅ Always filter on partition columns when reading

❌ Writing without coalesce (causing small files)  
✅ Call coalesce(N) before final write

❌ Using shuffle when not needed  
✅ Prefer coalesce over repartition when decreasing partitions

## 📚 Key Metrics to Track

- **Partition count** — Should match available cores
- **Data skew ratio** — Median partition size / Max partition size
- **Query execution time** — Before and after optimization
- **Shuffle bytes** — Lower is better
- **Output file count** — One per partition (ideal: 10-1000 files)

## 🎓 Next Steps for Learning

1. **Run both notebooks** with different configurations
2. **Experiment with your own data** using these patterns
3. **Analyze query plans** using `explain()`
4. **Monitor Spark UI** to see partition distribution
5. **Measure performance** improvements systematically
6. **Share findings** with your team

---

**Questions?** Review the detailed notebooks or Apache Spark documentation.