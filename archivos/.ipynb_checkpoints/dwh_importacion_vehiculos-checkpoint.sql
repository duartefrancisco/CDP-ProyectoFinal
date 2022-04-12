DROP TABLE IF EXISTS public.dim_aduana;
DROP TABLE IF EXISTS public.dim_fecha;
DROP TABLE IF EXISTS public.dim_tipo_importador;
DROP TABLE IF EXISTS public.dim_partida_arancelaria;
DROP TABLE IF EXISTS public.dim_pais;
DROP TABLE IF EXISTS public.dim_vehiculo;
DROP TABLE IF EXISTS public.fact_importaciones;

CREATE TABLE IF NOT EXISTS public.dim_aduana(
    aduana_sk INTEGER PRIMARY KEY,
  	aduana VARCHAR(75) NOT NULL,
  	nombre_corto VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS public.dim_fecha(
	fecha DATE PRIMARY KEY,
  	anio INTEGER NOT NULL,
  	mes INTEGER NOT NULL,
  	trimestre INTEGER NOT NULL,
  	dia INTEGER NOT NULL,
  	semana INTEGER NOT NULL,
  	dia_semana INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS public.dim_tipo_importador(
	tipo_importador_sk INTEGER PRIMARY KEY,
  	tipo_importador VARCHAR(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS public.dim_partida_arancelaria(
	partida_arancelaria_sk INTEGER PRIMARY KEY,
  	partida_arancelaria BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS public.dim_pais(
	pais_sk INTEGER PRIMARY KEY,
  	pais varchar(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS public.dim_vehiculo(
	vehiculo_sk INTEGER PRIMARY KEY,
  	modelo INTEGER NOT NULL,
  	marca VARCHAR(100) NOT NULL,
  	linea VARCHAR(100) NOT NULL,
  	centimetros_cubicos DECIMAL NULL,
  	distintivo VARCHAR(50) NULL,
  	tipo VARCHAR(50) NOT NULL,
  	combustible VARCHAR(50) NOT NULL,
  	asientos INTEGER NOT NULL,
  	puertas INTEGER NOT NULL,
  	tonelaje DECIMAL NOT NULL
);

CREATE TABLE IF NOT EXISTS public.fact_importaciones(
	fecha DATE NOT NULL,
  	pais_sk INTEGER NOT NULL,
  	aduana_sk INTEGER NOT NULL,
  	partida_arancelaria_sk INTEGER NOT NULL,
  	tipo_importador_sk INTEGER NOT NULL,
  	vehiculo_sk INTEGER NOT NULL,
  	valor_cif DECIMAL NOT NULL,
  	impuesto DECIMAL NOT NULL
);




