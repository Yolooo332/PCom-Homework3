Tema 2

Pachetele

Avem doua feluri de header, exact cum cere tema. Unul de date: protocol_id, conn_id, type, seq_num, len si dupa el vine payload-ul propriu zis. Si unul de control: protocol_id, conn_id, type, ack_num, recv_window. type poate sa fie DATA, ACK, SYN sau SYNACK. protocol_id e 42 si il verific peste tot, ca sa nu bag in seama gunoaie care pica de pe retea.

Conectarea (three way handshake)

Serverul sta si asculta pe portul 8032 (il leg prima data cand intra cineva in wait4connect, nu de la inceput). Clientul trimite un SYN acolo. Serverul cand vede SYN-ul isi deschide un socket nou pe un port random ales de sistem si trimite inapoi un SYN-ACK in care baga exact portul ala. Clientul ia portul din SYN-ACK, isi muta destinatia pe el si trimite ACK-ul final. De aici incolo toata vorba se face pe portul nou, ca sa pot avea mai multi clienti in acelasi timp fara sa se incurce unu cu altu.

Faza misto e ca daca se pierde ceva in timpul handshake-ului nu crapa. Amandoua partile au un timeout mic pe socket si daca nu vine nimic mai trimit inca o data SYN-ul sau SYN-ACK-ul. Si daca se pierde fix ACK-ul final, serverul oricum considera ca te-ai conectat in momentu in care ii pica primul segment de DATA, deci nu ramane blocat degeaba. Mai e si cazul in care unui client i se pierde SYN-ACK-ul si el retrimite SYN-ul: serverul isi da seama ca ala e deja conectat (dupa ip si port) si in loc sa faca o conexiune noua ii mai trimite o data SYN-ACK-ul vechi.

Transferul si fereastra (partea care conteaza la punctaj)

La sender: fiecare segment are un seq_num care creste. Eu tin o coada cu segmentele trimise dar neconfirmate (unacked) si nu las mai mult de 16 sa stea in aer in acelasi timp. 16 ori 512 octeti fac vreo 8K, cat sa umplu teava da sa nu sufoc bufferul de 9K al serverului. Cand imi vine un ACK, which e cumulativ (gen "am tot pana la N"), arunc din coada tot ce are seq mai mic decat N, fereastra aluneca mai in fata si bag segmente noi in locu lor. Daca un segment sta neconfirmat prea mult timp (am un timer pe conexiune) il retrimit, dar numa pe ala batran, nu toata coada, ca celelalte poate inca sunt pe drum si n-are rost sa le dublez.

La receiver: am un buffer de 9K. Daca imi pica fix segmentul pe care il asteptam (expected_seq) il bag in buffer si dau ACK. Daca vine unu din viitor (out of order) nu il arunc, il tin deoparte intr-un map dupa seq pana se umple gaura din fata, si dupa aia le pun pe toate la rand cum trebe. In fiecare ACK trimit si cat loc mai am liber in buffer (recv_window), deci si receiverul are fereastra lui, nu doar sender-ul.

De ce e optim: daca faceam stop-and-wait (trimit unu, astept ACK, trimit altu) mergea ca melcu, ca pe un link cu delay pierzi tot timpu doar stand si asteptand confirmarea. Asa, cu 16 segmente plecate deodata, tin teava plina si chiar se vede diferenta la viteza.

Thread-uri

Pe fiecare parte am un thread separat (handler) pornit din init_sender, respectiv init_receiver, care sta si asculta non stop ce vine de pe retea. Thread-ul principal e cel care apeleaza send_data / recv_data din aplicatie. Ca sa nu se calce pe picioare cele doua thread-uri am cate un mutex pe fiecare conexiune (con_lock) si toata modificarea de stare se face cu el luat.

Ma rog, cam asta e toata faza. Merge, trece testele si fisierele ies identice bit cu bit la final.
