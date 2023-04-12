Drop table if exists tipo_producto;
Drop table if exists conductor;
Drop table if exists wilaya;
Drop table if exists tipo_vehiculo; 
Drop table if exists camion; 



CREATE TABLE tipo_producto(
                    id_tipo_producto int PRIMARY KEY,
                    tipo_producto char not null UNIQUE, -- agregar descripción
                    descripcion car null
                    );
                    
CREATE TABLE conductor(
                    id_conductor int PRIMARY KEY,
                    conductor char not null UNIQUE
);

CREATE TABLE wilaya(
                    id_wilaya int PRIMARY KEY, 
                    wilaya char not null UNIQUE,
                    latitud char null, 
                    longitud char null, 
                    coordenadas char null
                    
);

CREATE TABLE tipo_vehiculo(
                    id_tipo_vehiculo int PRIMARY KEY,
                    tipo_vehiculo char not null UNIQUE, 
                    descripcion char null -- puede ser null?
                    
);
 

CREATE TABLE camion( 
                    id_camion int PRIMARY KEY, 
                    id_attsf int not null, 
                    id_wilaya int not null, 
                    id_tipo_vehiculo not null, 
                    marca char null, 
                    estado char not null,  -- se deja por futuros cambios
                    numero_tag char null, 
                    registro char null, 
                    chasis char null,  
                    institucion char null, 
                    espec_motor null, 
                    ano int null, -- tipo de dato   date/int
                    capacidad float null , -- ¿tipo de dato? float/decimal(10, 2)
                    modelo char null, 
                    fecha_alta date null, 
                    fecha_baja date null, 
                    propietario char null, 
                    FOREIGN KEY (id_wilaya) REFERENCES wilaya(id_wilaya),
                    FOREIGN KEY (id_tipo_vehiculo) REFERENCES tipo_vehiculo(id_tipo_vehiculo)
                    -- CHECK (estado IN ('Activo', 'Baja', 'Reserva')) 
);