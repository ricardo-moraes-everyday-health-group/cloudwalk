delete_all_countries_query = "delete from public.country"

insert_countries_query = """insert into public.country 
                            (id, name, iso3_code)
                            select distinct country_id, country_value, countryiso3code
                            from public.ny_gdp_mktp_cd_data"""

delete_all_gdp_query = "delete from public.gdp"

insert_gdp_query = """insert into public.gdp 
                      (country_id, "year", value)
                      select country_id, "date", value 
                      from public.ny_gdp_mktp_cd_data"""

delete_all_query = "delete from public.ny_gdp_mktp_cd_data"

insert_query = """insert into public.ny_gdp_mktp_cd_data
                  (country_id, country_value, countryiso3code, date, value, unit, obs_status, decimal_value)
                  values 
                  {records_list_template}"""

years_query = """select distinct "year" from public.gdp order by "year" desc limit 5"""

report_query = """
with piv_tb as (
	SELECT * FROM crosstab(
	'SELECT country_id, "year", SUM(value) 
	FROM public.gdp
 	WHERE "year" in ({years_query}) 
	GROUP BY country_id, "year"
	ORDER BY country_id, "year" desc'  
	) AS pivot_table("country_id" VARCHAR, {categories})),
countries_tb as (
	select  
		id,
		name,
		iso3_code
	from public.country)
select 
    t.id,
	t.name,
	t.iso3_code,
	{cols}
from piv_tb 
inner join countries_tb t on t.id = piv_tb.country_id"""

html_table_header = "<th>{h_desc}</th>"

html_table_col = "<td>{c_desc}</td>"

html_table_row = "<tr>{r_desc}</tr>"

html_full = """
<table>
    <caption>Gross Domestic Product</caption>
    <thead>
    {t_header}
    </thead>
    <tbody>
    {t_rows}
    </tbody>
</table>
<div>
    <p>Query</p>
    <pre><code>{qry}</code></pre>
</div>
"""