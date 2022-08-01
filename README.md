## What does this do?

Handles operations with the panel_database.

## What are typical use cases for this?

Generating panelapp dumps, generating django importing dumps, generating sql dumps.

Checking database data, checking tests against Test directory, checking gene transcripts.

## What is required for this to run?

- Python > 3.6
- Database called panel_database

<p align="center">
    <img height="100%" width="100%" src="panel_database_schema.png">
</p>

- From https://github.com/PyMySQL/mysqlclient-python :
>Linux  
Note that this is a basic step. I can not support complete step for build for all environment. If you can see some error, you should fix it by yourself, or ask for support in some user forum. Don't file a issue on the issue tracker.  
You may need to install the Python 3 and MySQL development headers and libraries like so:  
    &nbsp;&nbsp;&nbsp;&nbsp;$ sudo apt-get install python3-dev default-libmysqlclient-dev build-essential # Debian / Ubuntu  
    &nbsp;&nbsp;&nbsp;&nbsp;% sudo yum install python3-devel mysql-devel # Red Hat / CentOS

- Python packages specified in the requirements.txt

Usage:

```bash
source /home/panels/panel_env/bin/activate

# output all panelapp panels in folder ${day}_panelapp_dump
python main.py generate -all 
# output gms panelapp panels in folder ${day}_panelapp_dump
python main.py generate -gms 

# output django_fixtures/${day}/${day}_${table_name}.json and django_fixtures/${day}/${day}_json_dump.json
python main.py -t ${national_test_directory_xls} generate -j panels=${panelapp_dump_folder};${in-house_dump} g2t=${g2t_file}

# WARNING THE FOLLOWING 2 FILES DO NOT REFLECT WHAT IS STORED IN THE DATABASE
# RNA AND MITOCHONDRIAL GENES ARE REMOVED TO GENERATE REPORTS
# output sql_dump/${day}_genepanels.txt
python main.py generate -gp 
# output sql_dump/${day}_sample2genes.tsv
python main.py generate -m manifest.csv

# check db structure against panelapp dump
python main.py -t ${national_test_directory_xls} check panels=${panelapp_dump_folder};${in-house_dump} g2t=${g2t_file}

# import the data in the database
python main.py mod_db --hgnc ${hgnc_dump_file} ${admin_user} ${admin_passwd} -i django_fixtures/${day}/${day}_json_dump.json
# import hgnc data
python main.py mod_db --hgnc ${hgnc_dump_file} ${admin_user} ${admin_passwd} -hgnc hgnc=${hgnc_dump} date=${date}
# import g2t data
python main.py mod_db --hgnc ${hgnc_dump_file} ${admin_user} ${admin_passwd} -g2t ${g2t_file}
# import bespoke panels
python main.py mod_db --hgnc ${hgnc_dump_file} ${admin_user} ${admin_passwd} -new_panel ${xls_file}
# import new test directory
python panel_ops/main.py --hgnc ${hgnc_dump_file} mod_db ${admin_user} ${admin_passwd} --deploy_test_directory ${output_test_directory_parser} [ --ci_to_keep R-code R-code ... ]
```

## What does this output?

Panelapp dump of the day, django fixtures, sql dumps...
