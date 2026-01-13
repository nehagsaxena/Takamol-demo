This project creates a medallion architecture by
1. first creating bronze tables (Bronze table creation script)
2. loading synthetic data available in files zip folder for these tables (Load data to bronze + files (1).zip)
3. running the silver and gold tranformations -> gives a good lineage view (01_silver_layer_transformations.py, 02_gold_layer_transformations.py, 03_data_quality_checks.py)
4. crypto currency shows the streaming data - its md file gives more information (crypto_data_producer.ipynb - data producer notebook, crypto_streaming_analytics_live_demo.ipynb - for using autoloader and then see if the records from latest stream got inserted into the table)
5. agentic ai code -> run the driver notebook as is (05_register_uc_functions.py - has all the functions, driver (1).ipynb - for deployment of the agent)


<img width="417" height="222" alt="image" src="https://github.com/user-attachments/assets/5cdf790d-cfe5-4bfd-9c59-94966380b4c8" />
This is to show the lineage demo














<img width="284" height="642" alt="image" src="https://github.com/user-attachments/assets/d0b643ba-66f7-4460-8f4b-60bbafdea81c" />
for the Mosaic AI, Genie, Dashboards - used the takamol_demo catalog.

