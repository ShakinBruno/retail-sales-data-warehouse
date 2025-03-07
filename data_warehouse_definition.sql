DROP SCHEMA IF EXISTS SA_ONLINE     CASCADE;
DROP SCHEMA IF EXISTS SA_OFFLINE    CASCADE;
DROP SCHEMA IF EXISTS BL_CL         CASCADE;
DROP SCHEMA IF EXISTS BL_3NF        CASCADE;
DROP SCHEMA IF EXISTS BL_DM         CASCADE; 

\i './SA_ONLINE/src_online_retail_sales.sql'
\i './SA_OFFLINE/src_offline_retail_sales.sql'

\i './BL_CL/mta_loads.sql'
\i './BL_CL/mta_logs.sql'
\i './BL_CL/sp_cl_master_procedure.sql'

\i './BL_CL/t_map_product_categories.sql'
\i './BL_CL/t_map_product_subcategories.sql'
\i './BL_CL/t_map_products.sql'
\i './BL_CL/t_map_payment_methods.sql'
\i './BL_CL/t_map_card_types.sql'
\i './BL_CL/t_map_payment_details.sql'

\i './BL_3NF/ce_customers.sql'
\i './BL_3NF/ce_product_categories.sql'
\i './BL_3NF/ce_product_subcategories.sql'
\i './BL_3NF/ce_products.sql'
\i './BL_3NF/ce_regions.sql'
\i './BL_3NF/ce_countries.sql'
\i './BL_3NF/ce_cities.sql'
\i './BL_3NF/ce_addresses.sql'
\i './BL_3NF/ce_employees_scd.sql'
\i './BL_3NF/ce_payment_methods.sql'
\i './BL_3NF/ce_card_types.sql'
\i './BL_3NF/ce_payment_details.sql'
\i './BL_3NF/ce_devices.sql'
\i './BL_3NF/ce_sales.sql'

\i './BL_DM/dim_customers.sql'
\i './BL_DM/dim_products.sql'
\i './BL_DM/dim_locations.sql'
\i './BL_DM/dim_employees_scd.sql'
\i './BL_DM/dim_payment_details.sql'
\i './BL_DM/dim_devices.sql'
\i './BL_DM/dim_dates.sql'
\i './BL_DM/dim_times.sql'
\i './BL_DM/fct_sales.sql'