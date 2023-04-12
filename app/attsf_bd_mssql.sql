IF OBJECT_ID('LisData.tbl_ot_averia', 'U') IS NOT NULL
    DROP TABLE LisData.tbl_ot_averia;
IF OBJECT_ID('LisData.tbl_ot_repuesto', 'U') IS NOT NULL
    DROP TABLE LisData.tbl_ot_repuesto;
IF OBJECT_ID('LisData.tbl_ot', 'U') IS NOT NULL
    DROP TABLE LisData.tbl_ot;
IF OBJECT_ID('LisData.tbl_tipo_ot', 'U') IS NOT NULL
    DROP TABLE LisData.tbl_tipo_ot;
IF OBJECT_ID('LisData.tbl_averia', 'U') IS NOT NULL
    DROP TABLE LisData.tbl_averia;
IF OBJECT_ID('LisData.tbl_repuesto', 'U') IS NOT NULL
    DROP TABLE LisData.tbl_repuesto;  
IF OBJECT_ID('LisData.tbl_taller', 'U') IS NOT NULL
    DROP TABLE LisData.tbl_taller; 
IF OBJECT_ID('LisData.tbl_tipo_taller', 'U') IS NOT NULL
    DROP TABLE LisData.tbl_tipo_taller;
IF OBJECT_ID('LisData.tbl_frecuencia', 'U') IS NOT NULL
    DROP TABLE LisData.tbl_frecuencia;   
IF OBJECT_ID('LisData.tbl_distribucion', 'U') IS NOT NULL
    DROP TABLE LisData.tbl_distribucion;
IF OBJECT_ID('LisData.tbl_conductor', 'U') IS NOT NULL
    DROP TABLE LisData.tbl_conductor;
IF OBJECT_ID('LisData.tbl_disponibilidad', 'U') IS NOT NULL
    DROP TABLE LisData.tbl_disponibilidad;
IF OBJECT_ID('LisData.tbl_tipo_producto', 'U') IS NOT NULL
    DROP TABLE LisData.tbl_tipo_producto;
IF OBJECT_ID('LisData.tbl_camion', 'U') IS NOT NULL
    DROP TABLE LisData.tbl_camion;
IF OBJECT_ID('LisData.tbl_tipo_vehiculo', 'U') IS NOT NULL
    DROP TABLE LisData.tbl_tipo_vehiculo;
IF OBJECT_ID('LisData.tbl_wilaya', 'U') IS NOT NULL
    DROP TABLE LisData.tbl_wilaya;


CREATE TABLE tbl_averia
(
  id_averia int     NOT NULL,
  averia    varchar(250) NOT NULL,
  CONSTRAINT PK_tbl_averia PRIMARY KEY (id_averia)
)
GO

CREATE TABLE tbl_camion
(
  id_camion        int     NOT NULL,
  id_wilaya        int     NOT NULL,
  id_tipo_vehiculo int     NOT NULL,
  nombre_attsf     varchar(250),
  marca            varchar(250),
  estado           varchar(250),
  tag_number       varchar(250),
  registro         varchar(250),
  chasis           varchar(250),
  institucion      varchar(250),
  espec_motor      varchar(250),
  ano              int    ,
  capacidad        int    ,
  modelo           varchar(250),
  fecha_alta       date   ,
  fecha_baja       date   ,
  propietario      varchar(250),
  CONSTRAINT PK_tbl_camion PRIMARY KEY (id_camion)
)
GO

CREATE TABLE tbl_conductor
(
  id_conductor int     NOT NULL,
  conductor    varchar(250) NOT NULL,
  CONSTRAINT PK_tbl_conductor PRIMARY KEY (id_conductor)
)
GO

CREATE TABLE tbl_disponibilidad
(
  id_disponibilidad int    NOT NULL,
  id_camion         int    NOT NULL,
  fecha             date   NOT NULL,
  disponibilidad    binary NOT NULL,
  CONSTRAINT PK_tbl_disponibilidad PRIMARY KEY (id_disponibilidad)
)
GO

CREATE TABLE tbl_distribucion
(
  id_distribucion    int      NOT NULL,
  id_conductor       int      NOT NULL,
  id_tipo_producto   int      NOT NULL,
  id_camion          int      NOT NULL,
  id_wilaya          int      NOT NULL,
  salida_hora_fecha  datetime,
  llegada_hora_fecha datetime,
  km_salida          int     ,
  km_llegada         int     ,
  tm                 int     ,
  incidencias        varchar(750) ,
  observaciones      varchar(750) ,
  CONSTRAINT PK_tbl_distribucion PRIMARY KEY (id_distribucion)
)
GO

CREATE TABLE tbl_frecuencia
(
  id_frecuencia int     NOT NULL,
  frecuencia    varchar(250) NOT NULL,
  CONSTRAINT PK_tbl_frecuencia PRIMARY KEY (id_frecuencia)
)
GO

CREATE TABLE tbl_ot
(
  id_ot         int     NOT NULL,
  id_camion     int     NOT NULL,
  id_taller     int     NOT NULL,
  id_tipo_ot    int     NOT NULL,
  id_frecuencia int     NOT NULL,
  fecha_inicio  date    NOT NULL,
  fecha_fin     date   ,
  descripcion   varchar(250),
  observacion   varchar(250),
  CONSTRAINT PK_tbl_ot PRIMARY KEY (id_ot)
)
GO

CREATE TABLE tbl_ot_averia
(
  id_ot     int NOT NULL,
  id_averia int NOT NULL,
  CONSTRAINT PK_tbl_ot_averia PRIMARY KEY (id_ot, id_averia)
)
GO

CREATE TABLE tbl_ot_repuesto
(
  id_ot       int NOT NULL,
  id_repuesto int NOT NULL,
  CONSTRAINT PK_tbl_ot_repuesto PRIMARY KEY (id_ot, id_repuesto)
)
GO

CREATE TABLE tbl_repuesto
(
  id_repuesto int     NOT NULL,
  repuesto    varchar(250) NOT NULL,
  precio      float  ,
  CONSTRAINT PK_tbl_repuesto PRIMARY KEY (id_repuesto)
)
GO

CREATE TABLE tbl_taller
(
  id_taller      int     NOT NULL,
  id_wilaya      int     NOT NULL,
  id_tipo_taller int     NOT NULL,
  taller         varchar(250) NOT NULL,
  latitud        float  ,
  longitud       float  ,
  coordenadas    varchar(250),
  CONSTRAINT PK_tbl_taller PRIMARY KEY (id_taller)
)
GO

CREATE TABLE tbl_tipo_ot
(
  id_tipo_ot int     NOT NULL,
  tipo_ot    varchar(250) NOT NULL,
  CONSTRAINT PK_tbl_tipo_ot PRIMARY KEY (id_tipo_ot)
)
GO

CREATE TABLE tbl_tipo_producto
(
  id_tipo_producto int     NOT NULL,
  tipo_producto    varchar(250) NOT NULL,
  descripcion      varchar(250),
  CONSTRAINT PK_tbl_tipo_producto PRIMARY KEY (id_tipo_producto)
)
GO

CREATE TABLE tbl_tipo_taller
(
  id_tipo_taller int     NOT NULL,
  tipo_taller    varchar(250) NOT NULL,
  descripcion    varchar(250),
  CONSTRAINT PK_tbl_tipo_taller PRIMARY KEY (id_tipo_taller)
)
GO

CREATE TABLE tbl_tipo_vehiculo
(
  id_tipo_vehiculo int     NOT NULL,
  tipo_vehiculo    varchar(250) NOT NULL,
  descripcion      varchar(250),
  CONSTRAINT PK_tbl_tipo_vehiculo PRIMARY KEY (id_tipo_vehiculo)
)
GO

CREATE TABLE tbl_wilaya
(
  id_wilaya   int     NOT NULL,
  wilaya      varchar(250) NOT NULL,
  latitud     float  ,
  longitud    float  ,
  coordenadas varchar(250),
  CONSTRAINT PK_tbl_wilaya PRIMARY KEY (id_wilaya)
)
GO

ALTER TABLE tbl_ot_averia
  ADD CONSTRAINT FK_tbl_averia_TO_tbl_ot_averia
    FOREIGN KEY (id_averia)
    REFERENCES tbl_averia (id_averia)
GO

ALTER TABLE tbl_ot
  ADD CONSTRAINT FK_tbl_tipo_ot_TO_tbl_ot
    FOREIGN KEY (id_tipo_ot)
    REFERENCES tbl_tipo_ot (id_tipo_ot)
GO

ALTER TABLE tbl_ot
  ADD CONSTRAINT FK_tbl_frecuencia_TO_tbl_ot
    FOREIGN KEY (id_frecuencia)
    REFERENCES tbl_frecuencia (id_frecuencia)
GO

ALTER TABLE tbl_ot
  ADD CONSTRAINT FK_tbl_taller_TO_tbl_ot
    FOREIGN KEY (id_taller)
    REFERENCES tbl_taller (id_taller)
GO

ALTER TABLE tbl_ot_repuesto
  ADD CONSTRAINT FK_tbl_repuesto_TO_tbl_ot_repuesto
    FOREIGN KEY (id_repuesto)
    REFERENCES tbl_repuesto (id_repuesto)
GO

ALTER TABLE tbl_ot
  ADD CONSTRAINT FK_tbl_camion_TO_tbl_ot
    FOREIGN KEY (id_camion)
    REFERENCES tbl_camion (id_camion)
GO

ALTER TABLE tbl_disponibilidad
  ADD CONSTRAINT FK_tbl_camion_TO_tbl_disponibilidad
    FOREIGN KEY (id_camion)
    REFERENCES tbl_camion (id_camion)
GO

ALTER TABLE tbl_ot_averia
  ADD CONSTRAINT FK_tbl_ot_TO_tbl_ot_averia
    FOREIGN KEY (id_ot)
    REFERENCES tbl_ot (id_ot)
GO

ALTER TABLE tbl_ot_repuesto
  ADD CONSTRAINT FK_tbl_ot_TO_tbl_ot_repuesto
    FOREIGN KEY (id_ot)
    REFERENCES tbl_ot (id_ot)
GO

ALTER TABLE tbl_distribucion
  ADD CONSTRAINT FK_tbl_conductor_TO_tbl_distribucion
    FOREIGN KEY (id_conductor)
    REFERENCES tbl_conductor (id_conductor)
GO

ALTER TABLE tbl_distribucion
  ADD CONSTRAINT FK_tbl_tipo_producto_TO_tbl_distribucion
    FOREIGN KEY (id_tipo_producto)
    REFERENCES tbl_tipo_producto (id_tipo_producto)
GO

ALTER TABLE tbl_camion
  ADD CONSTRAINT FK_tbl_wilaya_TO_tbl_camion
    FOREIGN KEY (id_wilaya)
    REFERENCES tbl_wilaya (id_wilaya)
GO

ALTER TABLE tbl_camion
  ADD CONSTRAINT FK_tbl_tipo_vehiculo_TO_tbl_camion
    FOREIGN KEY (id_tipo_vehiculo)
    REFERENCES tbl_tipo_vehiculo (id_tipo_vehiculo)
GO

ALTER TABLE tbl_distribucion
  ADD CONSTRAINT FK_tbl_wilaya_TO_tbl_distribucion
    FOREIGN KEY (id_wilaya)
    REFERENCES tbl_wilaya (id_wilaya)
GO

ALTER TABLE tbl_distribucion
  ADD CONSTRAINT FK_tbl_camion_TO_tbl_distribucion
    FOREIGN KEY (id_camion)
    REFERENCES tbl_camion (id_camion)
GO

ALTER TABLE tbl_taller
  ADD CONSTRAINT FK_tbl_wilaya_TO_tbl_taller
    FOREIGN KEY (id_wilaya)
    REFERENCES tbl_wilaya (id_wilaya)
GO

ALTER TABLE tbl_taller
  ADD CONSTRAINT FK_tbl_tipo_taller_TO_tbl_taller
    FOREIGN KEY (id_tipo_taller)
    REFERENCES tbl_tipo_taller (id_tipo_taller)
GO
