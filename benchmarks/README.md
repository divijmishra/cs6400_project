# Running Write-Read benchmarks

1. **Generate writes for benchmark**:
To generate dummy ratings for the benchmark, run
```bash
python3 benchmarks/generate_write_data_for_benchmark.py
```

2. **Run benchmarks**:
To perform write-read benchmarks, run
```bash
python3 benchmarks/read_write_mysql.py
python3 benchmarks/read_write_neo4j.py
```
Ensure the credentials in the files are correct. You can set the experiment type (write-read ratio) in the ```EXPERIMENTS``` parameter in both ```read_write_....py``` files.
