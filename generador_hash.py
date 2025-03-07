import hashlib 

# FUNCION QUE RECIBE UN NOMBRE Y UNA FECHA Y DEVUELVE EL HASH MD5 DE LA CONCAT
def code(name, date): 
    texto = name + str(date) 
    h = hashlib.md5(texto.encode()) 
    # print("{} , {}, {}".format(name,date,h.hexdigest())) 
    print("{}".format(h.hexdigest())) 
    return str(h.hexdigest()) 

nombres =["U5C153"] 
# EJECUTA LA CODIFICACIÓN DEL ARREGLO nombres #2024-03-01(transformado en numerico o general da 45352.00) 
for nombre in nombres: 
    code(nombre, 45029.00)