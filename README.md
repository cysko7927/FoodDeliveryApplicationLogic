# FoodDeliveryApplicationLogic

Guida all'Avvio:

Innanzitutto bisogna avviare i brokers e inserire i Topics andando sulla cartella di kafka ed eseguire i seguenti script in ordine:\n
sh startAllBrokers.sh
sh script.sh

Se si vuole controllare se i topic sono stati inseriti correttamente eseguire lo script:ViewTopics.sh

Il client si avvia eseguendo il main dentro il file main.java.

Mentre gli altri componenti si avviano attraverso i main dei file:
UserServices.java
AuthenticationManager.java
OrderServices.java
ShippingServices.java

Nota tutti i componenti possono essere distribuiti su macchine diverse tranne UserServices e AuthenticationManager
che devono stare sulla stessa macchina perché devono accedere ai dati degli utenti.

I componenti appena avviati leggono il file config.txt per capire dove si trovano i brokers di kafka.

è obbligatorio quindi scrivere nel file config.txt gli indirrizzi ip e le porte corrette dei brokers e nel caso del Client
main è importante settare anche l'authentication manager.

Le righe del file config vanno scritte così: nameServer=Address:Port


#Nota
Non modificare i nameServers!!
I broker sono istanziati sempre sulle porte 9092 9093 9094
Mentre l'authentication manager sta sempre sulla porta 9091

Nella cartella DB sono presenti dei file txt dove vengono memorizzati i dati
in maniera persistente.
In particolare:
user.txt viene usato da AuthenticationManager e UserServices
items.txt and orders.txt vengono usati da OrderServices
shipment.txt viene usato da ShippingServices
