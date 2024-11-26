El proyecto conciste en la agrupación de cuentas de una MongoDB, cada cuenta contiene diferentes tipos
de operaciones en distintas fechas, el objetivo es analizar las cuentas y listar por cada cuenta
los distintios tipos de operaciones. Ademas existen dos colecciones más de MongoDB, amabas contienen
grupos por las que se estan agrupando las distintas cuentas de acuerdo a sus coleccion, es decir,
en primera se agrupa las cuentas de acuerdo a sus tipos de operación, si una cuenta tiene 5 tipos de
operacion el programa busca en la coleccion de segmentos a que segmentos tiene esa cuenta y agrega un
campo que indica los segmetos que tiene y otro campo que indica que operaciones no coinciden con algun
segemto de la coleccion.
Una vez que se tienen estos campos se realiza un segundo analisis con otra coleccion de la mongoDB,
donde se los segmentos anteriormente creados en las grupos.
