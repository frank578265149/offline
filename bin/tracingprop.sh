echo "create 'base_dealers',{NAME => 'traceFamily',COMPRESSION => 'snappy'}"|hbase shell
echo "create 'base_dealers_area',{NAME => 'traceFamily',COMPRESSION => 'snappy'}"|hbase shell
echo "create 'ent_area_sales_area_rela',{NAME => 'traceFamily',COMPRESSION => 'snappy'}"|hbase shell
echo "create 'base_product',{NAME => 'traceFamily',COMPRESSION => 'snappy'}"|hbase shell
echo "create 'base_product_property',{NAME => 'traceFamily',COMPRESSION => 'snappy'}"|hbase shell
echo "create 'stock_circulation',{NAME => 'traceFamily',COMPRESSION => 'snappy'}"|hbase shell