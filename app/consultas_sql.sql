select date_part('year', salida_fecha_hora), sum(tm) tm_totales 
from tbl_distribucion 
where salida_fecha_hora between '2021-01-01 00:00' and '2021-12-31 23:59'
group by date_part('year', salida_fecha_hora); 


