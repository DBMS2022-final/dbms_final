# dbms_final
DBMS final project

現在有很多跟time series相關的實驗,
多為呈現資料趨勢走向；
但是如果將所有資料都存入儲存空間,
則會過度消耗儲存空間。

我們code旨在將和time series相關的dataset,
以更精簡的方式呈現。

作法為:
選定dataset中以time為基準的連續3點X、Y、Z,
並計算兩兩鄉蓮點之斜率；
如果X,Y連線的斜率和Y,Z連線的斜率相同,
則將Y從dataset捨棄；
反之若有差異,
則儲存於dataset中