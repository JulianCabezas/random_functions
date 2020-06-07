# Function to intersept two large shapefiles
# The idea is to not have the RAM depleated


# Creo una secuencia para intersectar ambos shape para que no se me agote el RAM
# La secuencia va de 10000 en 10000 siguiendo el shape de uso de suelo
inter <- seq(1, nrow(shape_1), by = 10000)
inter
length(inter)

nrow(shape_1)

# Empieza la iteración
for (i in 1:length(inter)) {
  # Para evitar el error "subscrips out of bounds" hago un if,
  # que pesca el ultimo intervalo en funcion del numero de filas del shape
  print(i)
  if (i < length(inter)) {
    int1 <- inter[i]
    int2 <- inter[i + 1]
  } else {
    int1 <- inter[i]
    int2 <- nrow(shape_uso)
  }
  # Intersecta una parte del shape con el ddts
  int_sub <- st_intersection(shape_1[int1:int2, ], shape_2)
  # Junta un fragmento con el otro
  if (i == 1) {
    shape_fil <- int.sub
  } else {
    shape_fil <- rbind(shape_fil, int_sub)
  }
  if (i == 1) {
    print("Intersectando (Esto se va a demorar)")
  }
  print(paste(as.character(round(i / length(inter) * 100), 2), " % completado", sep = ""))
}

# Escribimos este resultado intermedio
st_write(shape_fil, ruta)
