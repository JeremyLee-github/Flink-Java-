# Flink-1.12 (Java)
使用Java code操作Flink

使用DataStream跟WaterMark搭配keyBy,Window與PcessFunction進行資料處理,或將DataStream與TableStream搭配Tab API和Flink SQL進行資料處理操作!!!
- 基本操作
- Kafka連線

- 1.Kafka對應連接輸入與輸出

- 2.模擬工業溫度感應器數據即時監控流程

  2.1(在10秒內溫度連去上升進行報警!!)
  
     Socket(數據輸出) -> Flink(keyby=>KeyedProcessFunction) -> 輸出報警

  2.2(高溫,低溫分流監控!!)
  
     Socket(數據輸出) -> Flink(keyby=>ProcessFunction) -> 輸出訊號
     依高溫和低溫分為主流與分流,使用ProcessFunction內Output方式分流輸出

3.電商用戶行為數據分析
  
  3.1即時熱門商品統計(統計1小時內的熱門商品,每五分鐘更新一次)
  
     DataStream(數據輸入)=>WaterMark=>KeyBy=>Window=>.aggregate( AggregateFunction(), WindowFunction())=>keyBy=>.process(KeyedProcessFunction())=>數據分析輸出
  
  3.2即時熱門頁面流量統計(統計每分鐘IP訪問流量,取出訪問量最大的前5個地址,每五秒鐘更新一次)
  
     DataStream(數據輸入)=>WaterMark=>KeyBy=>Window=>.aggregate( AggregateFunction(), WindowFunction())=>keyBy=>.process(KeyedProcessFunction())=>數據分析輸出
 
  3.3即時訪問流量統計(統計每小時的點擊訪問量(PV),並且對用戶進行去重(UV))
  
     DataStream(數據輸入)=>WaterMark=>filter=>Window=>使用HashSet或BloomFilter進行去重=>統計數據分析輸出
 
  3.4APP市場推廣統計(按照不同的推廣管道,分別統計數據)
  
     DataStream(數據輸入)=>WaterMark=>filter=>keyBy=>Window=>.aggregate( AggregateFunction(), ProcessWindowFunction())=>數據分析輸出

  3.5廣告頁面點擊流量統計(統計每小時頁面廣告的點擊量,每五秒更新一次,並按照不同區域進行劃分)
  
     DataStream(數據輸入)=>WaterMark=>keyBy=>Window=>.aggregate( AggregateFunction(), WindowFunction())=>數據分析輸出

  3.6廣告頁面廣告黑名單過濾(針對同一個廣告被同一用戶ID,對於"刷單"式的頻繁點擊行為進行過濾,並將該用戶加入黑名單,進行報警)
  
     DataStream(數據輸入)=>WaterMark=>(同一個用戶點擊同一個廣告的行為進行檢測報警過濾)=>keyBy=>Window=>.aggregate( AggregateFunction(), WindowFunction())=>數據分析輸出
  
  3.7惡意登入監控(針對同一用戶(可以不同IP)在兩秒內連續登入失敗,進行報警) => 使用CEP匹配模式對應
  
     DataStream(數據輸入)=>WaterMark=>CEP.pattern() 使用CEP匹配模式 =>patternStream.select(PatternSelectFunction())=>數據分析輸出

  3.8訂單支付失效監控(用戶下單後15分鐘未進行支付,則輸出監控訊息)
  
     DataStream(數據輸入)=>WaterMark=>CEP.pattern() 使用CEP匹配模式 =>patternStream.select(outputTag, PatternTimeoutFunction(), PatternSelectFunction())=>數據分析輸出

  3.9付款即時對帳監控(用戶下單並完成支付,應查詢其到帳訊息,進行即時帳單支付比對,對有不符合支付訊息跟到帳訊息的資料,進行輸出報警)
  
     connect:(針對異常報警處理!!)
     DataStream1(數據輸入).keyBy()
                                   =>合併.connect()=>.process(CoProcessFunction())==>數據分析輸出
     DataStream2(數據輸入).keyBy()

     intervalJoin:(只能輸出正常匹配事件)
     DataStream1(數據輸入).keyBy()
                                   =>合併.intervalJoin()=>.process(ProcessJoinFunction())==>數據分析輸出
     DataStream2(數據輸入).keyBy()
