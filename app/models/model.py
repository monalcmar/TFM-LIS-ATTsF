from sqlalchemy.ext.automap import automap_base
# =========== Modulos propios ===========
from db.connection import engine, conn


# Crear la clase base ORM
Base = automap_base()
# Reflejar las tablas de la base de datos en la clase base ORM
Base.prepare(engine, reflect=True)

# tablas = Base.classes.keys()
# print(tablas)

# Acceder a las tablas de la base de datos a través de su clase ORM
Averia = Base.classes.tbl_averia
Frecuencia = Base.classes.tbl_frecuencia
Taller = Base.classes.tbl_taller
Ot = Base.classes.tbl_ot
Camion = Base.classes.tbl_camion
Tipo_vehiculo = Base.classes.tbl_tipo_vehiculo
Wilaya = Base.classes.tbl_wilaya
Repuesto = Base.classes.tbl_repuesto
Personal = Base.classes.tbl_personal
Disponibilidad = Base.classes.tbl_disponibilidad
Distribucion = Base.classes.tbl_distribucion
Tipo_producto = Base.classes.tbl_tipo_producto
Tipo_taller = Base.classes.tbl_tipo_taller
Tipo_ot = Base.classes.tbl_tipo_ot