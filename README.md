# 🚀 My PySpark Learning Journey: From Fundamentals to Production Optimization

A comprehensive exploration of Apache Spark's core distributed computing concepts, data organization strategies, and performance optimization techniques. This repository documents my progression through real-world scenarios using actual datasets.

[![Python](https://img.shields.io/badge/Python-3.7%2B-blue)](https://www.python.org/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.0%2B-green)](https://spark.apache.org/)
[![License](https://img.shields.io/badge/License-MIT-yellow)](LICENSE)

## 📚 What's Inside

### Notebook 1: [Spark Partitioning — Estimating Partition Count for File Read](0-Understand%20Partitions.ipynb)

**Focus:** Understanding how Spark calculates optimal partition distribution

**What You'll Learn:**
- How Spark estimates partitions based on file size and available cores
- The relationship between `spark.sql.files.maxPartitionBytes` and partition count
- Practical calculation of partition distribution
- Impact of parallelism on data processing efficiency

**Key Skills:**
- Spark configuration parameters
- Partition calculation algorithms
- Default parallelism concepts

---

### Notebook 2: [Spark Partitioning & Bucketing — Exhaustive Tutorial](1-Bucketing.ipynb) ⭐

**Focus:** Comprehensive guide to data organization strategies in distributed computing

**What You'll Learn:**

| Section | Concepts Covered |
|---------|-----------------|
| **File-System Partitioning** | `partitionBy` strategies, directory hierarchies, multi-column partitioning |
| **In-Memory Partitioning** | `repartition` vs `coalesce`, shuffle operations, data distribution |
| **Partition Pruning** | Query optimization, directory skipping, performance impact |
| **Bucketing** | Hash-based organization, high-cardinality columns, join optimization |
| **Decision Matrix** | When to use each strategy, trade-offs and best practices |
| **Pitfalls & Solutions** | Small files problem, data skew, schema management |

**Real-World Datasets:**
- **Sales transactions** (100K+ rows) — date/region partitioning demo
- **User activity logs** (50K+ rows) — bucketing on user_id
- **Employee records** (20K rows) — multi-level partitioning strategies

**Key Skills:**
- Distributed data organization
- Query optimization techniques
- Join strategy optimization
- Performance tuning
- Data pipeline design

---

## 🎯 Learning Progression

```
Beginner → Intermediate → Advanced
   ↓
Part 1: How does Spark partition data?
   ↓
Part 2: How can I optimize data organization?
   ↓
Master: Building efficient distributed pipelines
```

## 🛠️ Tech Stack

- **Apache Spark** 3.0+ (PySpark)
- **Python** 3.7+
- **Parquet** file format
- **Local Mode** for learning (easily scalable to cluster mode)

## 🚀 Getting Started

### Prerequisites

```bash
# Install Spark
# Download from: https://spark.apache.org/downloads.html

# Install Python dependencies
pip install pyspark pandas
```

### Running the Notebooks

```bash
# Start Jupyter
jupyter notebook

# Open either notebook and run cells sequentially
```

### Quick Setup

```python
# Example from notebooks
import findspark
findspark.init('path/to/spark-3.0.1-bin-hadoop2.7')

from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("LearningApp") \
    .master("local[*]") \
    .getOrCreate()
```

## 💡 Key Concepts Covered

### Data Organization Strategies

**When to `partitionBy`:**
- Low-cardinality columns (region, date, department)
- < 100 distinct values
- Need directory-level pruning
- Building data lakes

**When to use `Bucketing`:**
- High-cardinality columns (user_id, session_id)
- > 1000 distinct values
- Frequent joins on column
- Avoid small files problem

### Performance Optimization

1. **Partition Pruning** — Skip irrelevant data directories at read time
2. **Bucket-Aware Joins** — Eliminate shuffle on bucketed columns
3. **Coalesce before Write** — Reduce small files
4. **Data Skew Mitigation** — Even distribution across partitions
5. **Query Plan Analysis** — Understanding Spark explain() output

## 📊 Sample Code Snippets

### Partition by Strategy
```python
# Write data partitioned by region and date
(df_sales
 .write
 .mode('overwrite')
 .format('parquet')
 .partitionBy('region', 'date')
 .save('output_path'))

# Spark automatically prunes non-matching directories
df_filtered = spark.read.parquet('output_path') \
    .filter(col('region') == 'North')  # Reads only North/ directory
```

### Bucketing Strategy
```python
# Write bucketed table for efficient joins
(df_logs
 .write
 .mode('overwrite')
 .bucketBy(8, 'user_id')
 .sortBy('timestamp')
 .saveAsTable('logs_bucketed'))

# Join without shuffle on bucketed key
df_joined = spark.table('logs_bucketed') \
    .join(spark.table('user_meta_bucketed'), 'user_id')
```

### Understanding Partitions
```python
# Check current partition count
df.rdd.getNumPartitions()

# Repartition for more parallelism
df_repart = df.repartition(8)

# Coalesce to reduce partitions (no shuffle)
df_coal = df.coalesce(3)

# Repartition by key for better join performance
df_key = df.repartition(6, 'join_column')
```

## 📈 Performance Metrics

Real-world improvements demonstrated:
- **Partition Pruning:** Reduces scan time by 60-80% for filtered queries
- **Bucket-Aware Joins:** Eliminates shuffle stage, reduces memory usage
- **Optimal Partitioning:** Achieves near-linear scaling with executor count

## 🎓 Learning Outcomes

After working through these notebooks, you'll be able to:

✅ Design optimal data organization strategies for various use cases  
✅ Understand and analyze Spark query execution plans  
✅ Implement partition pruning for query optimization  
✅ Choose between partitioning vs bucketing based on data characteristics  
✅ Identify and fix common partitioning mistakes  
✅ Tune Spark configurations for your specific workload  
✅ Analyze data skew and implement mitigation strategies  

## 🔧 Configuration Reference

```python
# Key Spark configurations for partitioning

spark.conf.set("spark.sql.shuffle.partitions", "200")  # Default
spark.conf.set("spark.sql.files.maxPartitionBytes", "128m")
spark.conf.set("spark.sql.files.openCostInBytes", "4m")
spark.conf.set("spark.default.parallelism", cpu_count)
```

## ⚠️ Common Pitfalls (and How to Avoid Them)

| Pitfall | Impact | Solution |
|---------|--------|----------|
| Partitioning by high-cardinality column | Millions of tiny files | Use bucketing instead |
| Ignoring data skew | Slow executor bottlenecks | Monitor Spark UI; use salting |
| Too many partition columns | Directory explosion | Limit to 2-3 columns max |
| Not using coalesce before write | Small files problem | Call `coalesce()` before write |
| Bucketing without same bucket count on joins | Unnecessary shuffle | Align bucket counts across tables |

## 📖 Recommended Reading

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Spark SQL Guide - Partitioning](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)
- [Bucketing Best Practices](https://docs.databricks.com/optimizations/bucketing.html)

## 🤝 Connect With Me

- **GitHub:** Explore my other projects
- **LinkedIn:** [Your Profile]
- **Email:** [Your Email]

## 📝 Notes for Recruiters

This repository demonstrates:
- **Deep technical understanding** of distributed computing fundamentals
- **Practical problem-solving** skills with real-world datasets
- **Clear communication** of complex concepts through documentation
- **Performance optimization** mindset and implementation
- **Learning agility** through structured exploration of advanced topics
- **Production-ready** code practices and best practices awareness

*Perfect for roles in: Data Engineering • Big Data • Apache Spark • Cloud Data Platforms • ETL/ELT Pipeline Development*

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

**Last Updated:** June 2024  
**Spark Version:** 3.0.1+  
**Status:** ✅ Active Learning — regularly updated with new insights