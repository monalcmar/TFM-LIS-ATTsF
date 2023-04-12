Drop table if exists tbl_ot_averia;
Drop table if exists tbl_ot_repuesto;
Drop table if exists tbl_ot;
Drop table if exists tbl_mecanico;
Drop table if exists tbl_tipo_ot;
Drop table if exists tbl_averia; 
Drop table if exists tbl_repuesto;
Drop table if exists tbl_taller;
Drop table if exists tbl_tipo_taller;
Drop table if exists tbl_frecuencia;
Drop table if exists tbl_distribucion;
Drop table if exists tbl_conductor;
Drop table if exists tbl_disponibilidad;
Drop table if exists tbl_tipo_producto;
Drop table if exists tbl_camion;
Drop table if exists tbl_tipo_vehiculo; 
Drop table if exists tbl_wilaya;


CREATE TABLE tbl_averia
(
  id_averia int     NOT NULL,
  averia    varchar(250) NOT NULL,
  PRIMARY KEY (id_averia)
);

CREATE TABLE tbl_camion
(
  id_camion        int     NOT NULL,
  nombre_attsf     varchar(250),
  id_wilaya        int	  ,
  id_tipo_vehiculo int     NOT NULL,
  marca            varchar(250),
  modelo           varchar(250),
  estado           varchar(250),
  numero_etiqueta       varchar(250),
  registro         varchar(250),
  chasis           varchar(250),
  institucion      varchar(250),
  espec_motor     varchar(250),
  ano              int    ,
  capacidad        int    , 
  fecha_alta       date   ,
  fecha_baja       date   ,
  propietario      varchar(250),
  PRIMARY KEY (id_camion)
);

CREATE TABLE tbl_conductor
(
  id_conductor int     NOT NULL,
  conductor    varchar(250) NOT NULL,
  PRIMARY KEY (id_conductor)
);

CREATE TABLE tbl_disponibilidad
(
  id_disponibilidad int    NOT NULL,
  id_camion         int    NOT NULL,
  fecha             date   NOT NULL,
  disponibilidad    boolean  NOT NULL,
  PRIMARY KEY (id_disponibilidad)
);

CREATE TABLE tbl_distribucion
(
  id_distribucion    int      NOT NULL,
  id_conductor       int      NOT NULL,
  id_tipo_producto   int      NOT NULL,
  id_camion          int      NOT NULL,
  id_wilaya          int      NOT NULL,
  no_serie           int     ,
  salida_fecha_hora timestamp,
  llegada_fecha_hora timestamp,
  km_salida          float     ,
  km_llegada         float     ,
  km_totales         float     ,
  tm                 float     ,
  incidencias        varchar(750) ,
  observaciones      varchar(750) ,
  PRIMARY KEY (id_distribucion)
);

CREATE TABLE tbl_frecuencia
(
  id_frecuencia int     NOT NULL,
  frecuencia    varchar(250) NOT NULL,
  PRIMARY KEY (id_frecuencia)
);

CREATE TABLE tbl_mecanico
(
  id_mecanico int     NOT NULL,
  mecanico    varchar(250) NOT NULL,
  id_taller   int    ,
  PRIMARY KEY (id_mecanico)
);

CREATE TABLE tbl_ot
(
  id_ot         int     NOT NULL,
  ot            int    ,
  id_camion     int     NOT NULL,
  id_taller     int    ,
  id_tipo_ot    int     NOT NULL,
  id_frecuencia int     NOT NULL,
  id_mecanico   int    ,
  fecha_inicio  date   ,
  fecha_fin     date   ,
  descripcion   varchar(750),
  observacion   varchar(750),
  PRIMARY KEY (id_ot)
);

CREATE TABLE tbl_ot_averia
(
  id_ot     int NOT NULL,
  id_averia int NOT NULL,
  PRIMARY KEY (id_ot, id_averia)
);

CREATE TABLE tbl_ot_repuesto
(
  id_ot       int NOT NULL,
  id_repuesto int NOT NULL,
  PRIMARY KEY (id_ot, id_repuesto)
);

CREATE TABLE tbl_repuesto
(
  id_repuesto int     NOT NULL,
  repuesto    varchar(250) NOT NULL,
  precio      float  ,
  PRIMARY KEY (id_repuesto)
);

CREATE TABLE tbl_taller
(
  id_taller      int     NOT NULL,
  id_wilaya      int     NOT NULL,
  id_tipo_taller int     NOT NULL,
  taller         varchar(250) NOT NULL,
  latitud        float  ,
  longitud       float  ,
  coordenadas    varchar(250),
  PRIMARY KEY (id_taller)
);

CREATE TABLE tbl_tipo_ot
(
  id_tipo_ot int     NOT NULL,
  tipo_ot    varchar(250) NOT NULL,
  PRIMARY KEY (id_tipo_ot)
);

CREATE TABLE tbl_tipo_producto
(
  id_tipo_producto int     NOT NULL,
  tipo_producto    varchar(250) NOT NULL,
  descripcion      varchar(250),
  PRIMARY KEY (id_tipo_producto)
);

CREATE TABLE tbl_tipo_taller
(
  id_tipo_taller int     NOT NULL,
  tipo_taller    varchar(250) NOT NULL,
  descripcion    varchar(250),
  PRIMARY KEY (id_tipo_taller)
);

CREATE TABLE tbl_tipo_vehiculo
(
  id_tipo_vehiculo int     NOT NULL,
  tipo_vehiculo    varchar(250) NOT NULL,
  descripcion      varchar(250),
  PRIMARY KEY (id_tipo_vehiculo)
);

CREATE TABLE tbl_wilaya
(
  id_wilaya   int     NOT NULL,
  wilaya      varchar(250) NOT NULL,
  latitud     float  ,
  longitud    float  ,
  coordenadas varchar(250),
  PRIMARY KEY (id_wilaya)
);

ALTER TABLE tbl_ot_averia
  ADD CONSTRAINT FK_tbl_averia_TO_tbl_ot_averia
    FOREIGN KEY (id_averia)
    REFERENCES tbl_averia (id_averia);

ALTER TABLE tbl_ot
  ADD CONSTRAINT FK_tbl_tipo_ot_TO_tbl_ot
    FOREIGN KEY (id_tipo_ot)
    REFERENCES tbl_tipo_ot (id_tipo_ot);

ALTER TABLE tbl_ot
  ADD CONSTRAINT FK_tbl_frecuencia_TO_tbl_ot
    FOREIGN KEY (id_frecuencia)
    REFERENCES tbl_frecuencia (id_frecuencia);

ALTER TABLE tbl_ot
  ADD CONSTRAINT FK_tbl_taller_TO_tbl_ot
    FOREIGN KEY (id_taller)
    REFERENCES tbl_taller (id_taller);

ALTER TABLE tbl_ot_repuesto
  ADD CONSTRAINT FK_tbl_repuesto_TO_tbl_ot_repuesto
    FOREIGN KEY (id_repuesto)
    REFERENCES tbl_repuesto (id_repuesto);

ALTER TABLE tbl_ot
  ADD CONSTRAINT FK_tbl_camion_TO_tbl_ot
    FOREIGN KEY (id_camion)
    REFERENCES tbl_camion (id_camion);

ALTER TABLE tbl_disponibilidad
  ADD CONSTRAINT FK_tbl_camion_TO_tbl_disponibilidad
    FOREIGN KEY (id_camion)
    REFERENCES tbl_camion (id_camion);

ALTER TABLE tbl_ot_averia
  ADD CONSTRAINT FK_tbl_ot_TO_tbl_ot_averia
    FOREIGN KEY (id_ot)
    REFERENCES tbl_ot (id_ot);

ALTER TABLE tbl_ot_repuesto
  ADD CONSTRAINT FK_tbl_ot_TO_tbl_ot_repuesto
    FOREIGN KEY (id_ot)
    REFERENCES tbl_ot (id_ot);

ALTER TABLE tbl_distribucion
  ADD CONSTRAINT FK_tbl_conductor_TO_tbl_distribucion
    FOREIGN KEY (id_conductor)
    REFERENCES tbl_conductor (id_conductor);

ALTER TABLE tbl_distribucion
  ADD CONSTRAINT FK_tbl_tipo_producto_TO_tbl_distribucion
    FOREIGN KEY (id_tipo_producto)
    REFERENCES tbl_tipo_producto (id_tipo_producto);

ALTER TABLE tbl_camion
  ADD CONSTRAINT FK_tbl_wilaya_TO_tbl_camion
    FOREIGN KEY (id_wilaya)
    REFERENCES tbl_wilaya (id_wilaya);

ALTER TABLE tbl_camion
  ADD CONSTRAINT FK_tbl_tipo_vehiculo_TO_tbl_camion
    FOREIGN KEY (id_tipo_vehiculo)
    REFERENCES tbl_tipo_vehiculo (id_tipo_vehiculo);

ALTER TABLE tbl_distribucion
  ADD CONSTRAINT FK_tbl_wilaya_TO_tbl_distribucion
    FOREIGN KEY (id_wilaya)
    REFERENCES tbl_wilaya (id_wilaya);

ALTER TABLE tbl_distribucion
  ADD CONSTRAINT FK_tbl_camion_TO_tbl_distribucion
    FOREIGN KEY (id_camion)
    REFERENCES tbl_camion (id_camion);

ALTER TABLE tbl_taller
  ADD CONSTRAINT FK_tbl_wilaya_TO_tbl_taller
    FOREIGN KEY (id_wilaya)
    REFERENCES tbl_wilaya (id_wilaya);

ALTER TABLE tbl_taller
  ADD CONSTRAINT FK_tbl_tipo_taller_TO_tbl_taller
    FOREIGN KEY (id_tipo_taller)
    REFERENCES tbl_tipo_taller (id_tipo_taller);
   
ALTER TABLE tbl_ot
  ADD CONSTRAINT FK_tbl_mecanico_TO_tbl_ot
    FOREIGN KEY (id_mecanico)
    REFERENCES tbl_mecanico (id_mecanico);

ALTER TABLE tbl_mecanico
  ADD CONSTRAINT FK_tbl_taller_TO_tbl_mecanico
    FOREIGN KEY (id_taller)
    REFERENCES tbl_taller (id_taller);
