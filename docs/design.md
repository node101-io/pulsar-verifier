# Pulsar Verifier Design

Bu doküman Pulsar Verifier'ın sorumluluklarını, komponentlerini ve bu komponentlerin birlikte nasıl çalıştığını tanımlar. Implementation sürecinde source-of-truth olarak kullanılacaktır.

Doküman iki soruya cevap verir:

1. Verifier node hangi komponentlerden oluşur ve her komponentin sınırı nedir?
2. Bir proof sisteme farklı yollardan geldiğinde hangi sırayla ne olur?

Bu dokümandaki `proof`, ZK proof'un ham byte'larını; `chain`, Pulsar blockchain uygulamasını; `consumer` ise proof'u verifier'a teslim eden uygulamayı ifade eder.

## 1. Verifier Nedir?

Pulsar Verifier, her validator'ın yanında çalışan bir proof verification sidecar'ıdır. Consensus node'un parçası değildir ve kendi başına consensus kararı vermez. Görevi proof'ları validator'lar arasında erişilebilir hale getirmek, doğru zamanda doğrulamak ve sonucu Pulsar chain'e bildirmektir.

Pulsar chain proof byte'larını veya ZK verifier implementasyonlarını doğrudan çalıştırmaz. Bunun yerine validator'ın yanındaki verifier node'a proof hash seti verir ve her hash için bir state ister. Validator, bu state'i vote extension içerisine koyarak consensus sürecine dahil eder.

Verifier'ın temel sorumlulukları şunlardır:

- Consumer'dan gelen proof ve signed transaction'ı validate etmek.
- Consumer'ın gönderdiği signed transaction'ı relayer olarak chain'e iletmek.
- Elindeki proof'ların erişilebilirliğini validator P2P ağına duyurmak.
- Chain'in istediği proof hash'leri için proof'u gerektiğinde başka validator'lardan almak.
- Proof type'a uygun verifier'ı seçip cryptographic verification çalıştırmak.
- Chain'e yalnızca `VERIFIED`, `WRONG` veya `UNAVAILABLE` state'lerini döndürmek.

Verifier şu sorumluluklara sahip değildir:

- Consensus kararı vermek.
- Chain'in transaction history'sini permanent database olarak saklamak.
- Chain'e girmemiş bir proof'u cryptographically verify ederek chain'e önceden raporlamak.
- P2P üzerinden transaction bytes yayınlamak.

### 1.1 Temel Kural

Proof'un local olarak mevcut olması availability announcement için yeterlidir. Chain'e girmiş olması veya verification'dan geçmiş olması gerekmez.

Buna karşılık başka peer'lardan proof istemek ve proof'u verify etmek için proof hash'inin chain tarafından verifier'a gönderilen aktif hash setinde bulunması gerekir.

```text
Local proof mevcut
    -> AvailabilityAnnouncement yayınla

Proof hash'i chain'in aktif hash setinde
    -> Gerekirse ProofContent iste
    -> ProofContent mevcutsa verification başlat
```

Bu iki kural birbirinden bağımsızdır. Availability, proof'un local olarak var olduğunu; chain hash seti ise proof'un verifier tarafından işleme alınması gerektiğini gösterir.

## 2. Sistem Mimarisi

Verifier aşağıdaki komponentlerden oluşur:

```text
                         Pulsar Validator
                                |
                 +--------------+--------------+
                 |                             |
          Chain integration                Chain query
          relayer/status client            request/response
                 |                             |
                 +--------------+--------------+
                                |
                        +-------v--------+
                        |  ProofService  |
                        | domain core    |
                        +---+--------+---+
                            |        |
                 +----------+        +-----------+
                 |                               |
          Verification                    Ephemeral storage
          registry/workers                Moka + Rust maps
                 |                               |
                 +---------------+---------------+
                                 |
                          P2P network
                 +---------------+---------------+
                 |                               |
             GossipSub                    Request-response
        availability messages             proof content exchange
```

Komponentlerin kısa sorumlulukları:

| Komponent | Sorumluluk |
| --- | --- |
| RPC boundary | Consumer ve chain request'lerini alır, response üretir; domain kararını vermez. |
| `ProofService` | Validation, lifecycle, cache, availability ve verification akışını koordine eder. |
| Chain integration | Signed `TxRaw` transaction'ını broadcast eder ve chain'in hash seti status contract'ını kullanır. |
| P2P driver | Authorized peer'larla availability gossip ve proof exchange yürütür. |
| Availability index | Bir proof hash'ini bildiğini söyleyen veya bu bilgiye sahip olan peer'ları tutar. |
| Verification registry/workers | Proof type'a göre verifier seçer ve verification task'larını çalıştırır. |
| Ephemeral storage | Proof bytes, geçici submission metadata'sı ve state'leri Moka/map üzerinde tutar. |
| Identity/authorization | PeerId'nin aktif validator'a ait olduğunu doğrular. |
| Runtime | Tüm komponentleri başlatır, channel'ları bağlar ve controlled shutdown sağlar. |

RPC, chain integration ve P2P driver network/protocol sınırlarını temsil eder. ProofService ise bu sınırların arasındaki domain kararlarının tek sahibidir.

## 3. Domain Modeli

### 3.1 Proof ve Proof Hash

Proof, ZK proof sisteminin ürettiği ham byte dizisidir.

```text
proof_hash = BLAKE3(proof_bytes)
```

Proof hash'i canonical content identifier'dır. Aşağıdaki yapılar hash'i anahtar olarak kullanır:

- Proof cache.
- Availability index.
- Active chain hash set.
- Verification state.
- Chain status response.

Hash mismatch bir verification failure değildir. Bu, proof ile transaction arasındaki binding'in geçersiz olduğunu gösteren input validation hatasıdır.

### 3.2 Proof Type

`proof_type`, proof'un hangi verifier implementasyonuna yönlendirileceğini belirler.

```text
mock    -> MockVerifier
groth16 -> Groth16Verifier
kimchi  -> KimchiVerifier
```

İlk submission sırasında proof type, signed `MsgSubmitProof` transaction'ından okunur. Desteklenmeyen type validation aşamasında reddedilir; storage'a alınmaz ve availability announcement yayınlanmaz.

### 3.3 `ProofSubmission`

`ProofSubmission`, consumer'ın RPC üzerinden ilk gönderdiği composite payload'dur.

```text
ProofSubmission
├── proof bytes
└── signed Cosmos TxRaw bytes
```

Önerilen protobuf şekli:

```proto
message ProofSubmission {
  bytes proof = 1;
  bytes tx_bytes = 2;
}
```

`tx_bytes`, proof ile birlikte gelen signed Cosmos `TxRaw` transaction'ıdır. Service transaction'dan `proof_hash` ve `proof_type` okur ve proof bytes ile binding validation yapar.

Bu mesaj yalnızca RPC/relayer boundary'sinde kullanılır. P2P proof exchange sırasında transaction bytes tekrar gönderilmez.

### 3.4 Placeholder `MsgSubmitProof`

Chain modülü hazır olana kadar aşağıdaki placeholder transaction proto'su kullanılır:

```proto
message MsgSubmitProof {
  string sender = 1;
  bytes proof_hash = 2;
  string proof_type = 3;
}
```

Transaction'ın beklenen yapısı:

```text
TxRaw
└── TxBody
    └── Any
        └── MsgSubmitProof
```

Service şu kuralları uygular:

- `TxRaw` decode edilebilmelidir.
- `TxBody` decode edilebilmelidir.
- Transaction tam olarak bir `MsgSubmitProof` içermelidir.
- `Any.type_url` beklenen placeholder type URL olmalıdır.
- `proof_hash` beklenen 32 byte uzunluğunda olmalıdır.
- `proof_type` boş olmamalıdır ve registry'de desteklenmelidir.
- `BLAKE3(proof_bytes) == proof_hash` olmalıdır.

Gerçek chain modülü hazır olduğunda placeholder, chain-owned proto contract'ı ile değiştirilecektir.

### 3.5 `ProofContent`

P2P üzerinden gönderilen proof içeriği `ProofContent` olarak adlandırılır.

```proto
message ProofContent {
  bytes proof_hash = 1;
  bytes proof = 2;
}
```

`ProofContent` transaction bytes içermez. Response içindeki hash, alınan proof bytes'ın istenen içerikle eşleştiğini kontrol etmek için taşınır.

### 3.6 Active Chain Hash Set

Chain verifier'a yalnızca proof hash'lerinden oluşan bir set gönderir:

```text
ChainProofSet
└── repeated proof_hash
```

Bu set verifier'ın chain-side processing gate'idir. Hash setinde bulunmayan local proof:

- Verify edilmez.
- Başka peer'dan istenmez.
- Chain status response'una dahil edilmez.

Hash seti permanent database'e yazılmaz. Status request sırasında işlenir ve gerekirse kısa ömürlü in-memory active set olarak tutulur.

### 3.7 Proof Type Metadata Kararı

Chain yalnızca hash seti gönderdiği için P2P üzerinden proof'u indiren node'un `proof_type` bilgisini nereden alacağı ayrıca çözülmelidir. Hash tek başına proof type içermez.

Implementation başlamadan önce aşağıdaki seçeneklerden biri seçilmelidir:

1. Proof formatı type bilgisini deterministically içerir.
2. `ProofContent` içine `proof_type` eklenir ve bu metadata için güvenilir bir binding tanımlanır.
3. Chain status request'i hash ile birlikte type bilgisini de taşır. Bu, chain'in yalnız hash seti gönderme kararını değiştirir.
4. Proof hash hesaplamasına proof type dahil edilir. Bu durumda hash contract'ı değişir.

Bu karar verilmeden, proof'u indiren node'un doğru verifier'ı deterministically seçmesi garanti edilemez. Bu nedenle implementation sırasında sessiz bir varsayım yapılmamalıdır.

## 4. RPC Komponenti

RPC boundary dış sistemlerle verifier arasındaki giriş/çıkış noktasıdır. RPC handler yalnızca protobuf dönüşümü, authentication boundary'si ve service çağrısından sorumludur. Hash binding, proof type desteği ve lifecycle kararları `ProofService` içinde kalır.

RPC'nin üç ana yüzü vardır:

1. Consumer Submit RPC.
2. Chain Status Query RPC.
3. Health RPC.

### 4.1 Consumer Submit RPC

Consumer şu payload'ı gönderir:

```text
SubmitProof(ProofSubmission)
```

Başarılı akış:

1. RPC request protobuf'a decode edilir.
2. `ProofService` transaction/proof binding validation yapar.
3. Proof bytes geçici Moka/in-memory state'e yazılır.
4. Proof hash'i için availability announcement hemen yayınlanır.
5. Signed `TxRaw` chain relayer üzerinden broadcast edilir.
6. Consumer'a relayer sonucu ve proof'un henüz verification'dan geçmediğini belirten pending response döner.

Availability announcement ile chain broadcast birbirinden bağımsızdır. Node proof bytes'a sahipse announcement yayınlayabilir; bu announcement proof'un chain'e girdiği veya doğru olduğu anlamına gelmez.

Validation başarısızsa hiçbir side effect oluşmaz:

```text
invalid submission
    -> no cache write
    -> no availability announcement
    -> no chain broadcast
```

Validation başarılı olup chain broadcast başarısız olursa consumer'a relayer hatası döner. Proof local cache'te kalabilir ve announcement daha önce yayınlanmış olabilir; ancak chain hash setinde görünmediği sürece bu proof için retrieval veya verification başlatılmaz.

### 4.2 Chain Status Query RPC

Chain vote extension hazırlarken verifier'a proof hash setini gönderir:

```text
GetProofStatuses
└── repeated proof_hashes
```

Verifier aynı hash'ler için state döndürür:

```text
ProofStatus
├── proof_hash
└── state: VERIFIED | WRONG | UNAVAILABLE
```

Chain verifier'dan proof bytes, transaction bytes, proof type metadata'sı veya availability provider listesi istemez. Chain ile verifier arasındaki tek query contract'ı hash seti ve bu hash setine karşılık gelen state'lerdir.

State anlamları:

- `VERIFIED`: ProofContent elde edilmiş ve seçilen verifier proof'u doğru bulmuştur.
- `WRONG`: ProofContent elde edilmiş, ancak cryptographic verification başarısız olmuştur.
- `UNAVAILABLE`: Proof local'de yoktur, authorized provider'dan alınamamıştır, proof type güvenilir biçimde belirlenememiştir veya status response deadline'ı içinde sonuç üretilememiştir.

`UNAVAILABLE`, proof'un yanlış olduğu anlamına gelmez. Yalnızca verifier'ın o query için doğrulanabilir bir proof sonucu üretemediğini gösterir.

### 4.3 Health RPC

Health RPC en azından şu durumları raporlayabilmelidir:

- Process ayakta mı?
- Chain relayer/status client hazır mı?
- P2P driver çalışıyor mu?
- Validator authorization set yüklenmiş mi?
- Verification worker'ları çalışıyor mu?

## 5. Chain Integration Komponenti

Chain integration iki farklı görevi tek komponent altında toplar:

1. Consumer'dan gelen signed `TxRaw` transaction'ını relayer olarak chain'e göndermek.
2. Chain'in gönderdiği proof hash setini alıp state response üretmek.

Bu komponent transaction history veya permanent chain record tutmaz.

### 5.1 Relayer Akışı

```text
Consumer
   |
   | ProofSubmission { proof, tx_bytes }
   v
ProofService validation
   |
   +--> temporary proof cache
   |
   +--> AvailabilityAnnouncement
   |
   `--> Chain relayer --> Pulsar chain
```

Transaction'ın chain'e kabul edilmesi ile proof'un `VERIFIED` olması iki farklı işlemdir. Relayer yalnızca signed transaction'ı chain'e iletir; proof verification daha sonra chain status query hash seti ile tetiklenir.

### 5.2 Status Query Akışı

```text
Pulsar chain
   |
   | repeated proof_hashes
   v
ProofService
   |
   +--> local ProofContent lookup
   +--> availability lookup or query
   +--> proof exchange if required
   `--> state per proof_hash
```

Chain'in gönderdiği hash seti active chain set olarak işlenir. Her hash için local proof mevcutsa verification planlanır. Proof yoksa availability index kullanılır; provider bilinmiyorsa `AvailabilityQuery` yayınlanır.

MVP'de verifier `ProofSubmission` transaction'larını CometBFT event/WebSocket listener ile takip etmez. Chain'e dahil olma bilgisi, chain'in daha sonra gönderdiği hash seti üzerinden öğrenilir. Hash seti dışında ayrı bir chain event akışı veya kalıcı transaction record'u tutulmaz.

## 6. ProofService Komponenti

`ProofService`, RPC, chain integration ve P2P driver arasındaki merkezi application/domain service'tir. Dış komponentler karar vermez; event veya command'i service'e iletir.

### 6.1 ProofService Sorumlulukları

- `ProofSubmission` validation.
- `ProofContent` validation.
- Transaction hash ile proof hash binding kontrolü.
- Supported proof type kontrolü.
- Temporary proof cache ve lifecycle state yönetimi.
- Availability announcement tetikleme.
- Active chain hash set ile proof eşleştirme.
- Provider seçimi ve proof exchange başlatma.
- Proof type'a göre verifier seçme.
- Verification task deduplication.
- Chain status response üretme.

### 6.2 Validation ile Verification Ayrımı

Validation, input'ın yapısal ve contract'a uygun olup olmadığını kontrol eder. RPC submission geldiği anda yapılır:

- TxRaw decode ediliyor mu?
- Beklenen MsgSubmitProof mevcut mu?
- Proof hash doğru mu?
- Proof type boş veya unsupported mı?

Verification ise proof'un cryptographic olarak doğru olup olmadığını kontrol eder. Bu işlem RPC submission'ın hemen ardından çalışmaz. Yalnızca proof hash'i active chain hash setinde görüldükten ve ProofContent mevcut olduktan sonra başlatılır.

```text
RPC submission
    -> validation
    -> temporary storage
    -> availability announcement
    -> no cryptographic verification yet

Chain hash set
    -> retrieval if needed
    -> cryptographic verification
    -> status response
```

### 6.3 Lifecycle

`UNAVAILABLE`, chain'e dönen query state'idir; aşağıdaki internal lifecycle state'lerinin bir sonucu olarak üretilebilir.

```text
Unknown
   |
   | proof local'e geldi
   v
OffChainAvailable
   |
   | hash active chain setinde görüldü
   +----------------------+
   |                      |
   | proof local'de       | proof local'de yok
   v                      v
Available           ChainRequestedMissing
   |                      |
   | verification         | provider lookup/query
   | başlatıldı           | ve proof exchange
   v                      |
Verifying <--------------+
   |
   +------------------+
   |                  |
   v                  v
Verified             Wrong
```

Kurallar:

- `OffChainAvailable` proof yalnızca availability için tutulur; verify edilmez.
- `ChainRequestedMissing` state'inde yalnızca active hash setindeki proof için retrieval başlatılır.
- Proof alındığında response hash'i ve BLAKE3 hash'i tekrar kontrol edilir.
- Verification task tamamlanmadan chain query gelirse state `UNAVAILABLE` dönebilir.
- Aynı proof hash'i için eşzamanlı duplicate verification başlatılmaz.

## 7. P2P Komponenti

P2P ağı proof CDN'i gibi davranır; ancak iki ayrı veri türünü farklı protokollerle taşır:

1. Proof availability bilgisi.
2. İhtiyaç duyulan proof content'i.

P2P üzerinde `ProofSubmission` veya signed `TxRaw` yayınlanmaz. P2P node'lar proof'un kendisini ve proof'un kimlerde olduğuna dair bilgiyi taşır.

### 7.1 Availability Protocol: GossipSub

GossipSub üzerinde küçük availability mesajları yayınlanır:

```proto
message AvailabilityAnnouncement {
  bytes proof_hash = 1;
}

message AvailabilityQuery {
  bytes request_id = 1;
  bytes proof_hash = 2;
}

message AvailabilityResponse {
  bytes request_id = 1;
  bytes proof_hash = 2;
  repeated bytes provider_peer_ids = 3;
}
```

Wire-level ayrım için envelope kullanılabilir:

```proto
message AvailabilityMessage {
  oneof payload {
    AvailabilityAnnouncement announcement = 1;
    AvailabilityQuery query = 2;
    AvailabilityResponse response = 3;
  }
}
```

#### Announcement

Node local proof bytes'a sahip olduğu anda announcement yayınlar. Bu şu durumlarda geçerlidir:

- Validated RPC submission local cache'e alındığında.
- P2P exchange ile geçerli ProofContent alındığında.

Announcement proof'un chain'e girdiğini veya doğru olduğunu iddia etmez. Yalnızca announcement'ı yayınlayan authenticated peer'ın ilgili proof bytes'a sahip olduğunu bildirir.

#### Query

`AvailabilityQuery`, “Bu proof'a sahip olduğunu söyleyen birini biliyor musunuz?” sorusudur. Query'yi gören her authorized peer kendi availability map'ine bakar.

#### Response

Query'yi gören peer, kendi map'inde ilgili hash için bildiği provider peer ID listesini response olarak yayınlar. Response'u gönderen peer'ın proof'a sahip olması gerekmez.

Response'u alan node `provider_peer_ids` listesini kendi availability index'ine ekler ve doğrudan listedeki peer'lardan birine proof exchange request gönderebilir.

### 7.2 Proof Exchange: Request-Response

Availability map proof'un kendisini içermez. Proof bytes almak için doğrudan seçilen peer'a request-response yapılır:

```proto
message GetProofRequest {
  bytes proof_hash = 1;
}

message GetProofResponse {
  oneof result {
    ProofContent content = 1;
    ProofNotFound not_found = 2;
  }
}

message ProofNotFound {}
```

Proof exchange yalnızca şu koşulda başlatılır:

```text
proof_hash ∈ active_chain_hash_set
```

Response alındığında requester şunları kontrol eder:

1. Requested hash ile response hash'i aynı mı?
2. `BLAKE3(proof)` response hash'i ile aynı mı?
3. Proof type güvenilir biçimde belirlenebiliyor mu?
4. İçerik local cache'e alınabilir mi?

ProofNotFound response provider'ı availability map'ten çıkarmak için kullanılabilir. Geçersiz content response'u verification sonucu olarak `WRONG` sayılmaz; exchange/content validation hatasıdır.

### 7.3 Availability Index

Availability index ephemeral bir network bilgisidir:

```text
proof_hash -> Set<PeerId>
```

Index şu kaynaklardan güncellenir:

- Announcement'ı yayınlayan source peer.
- AvailabilityResponse içindeki provider peer ID'leri.
- Başarılı proof exchange source peer'ı.
- Peer disconnect event'i.
- Peer authorization değişikliği.

Kayıtlar stale olabilir. Bu nedenle:

- Peer disconnect olduğunda ilgili ID temizlenir.
- Kayıtlara TTL uygulanabilir.
- `ProofNotFound` sonrası provider çıkarılır.
- Başarısız download reliability metriğine yazılabilir.

### 7.4 Libp2p Driver ve Transport

P2P driver `Swarm` yaşam döngüsünün sahibidir. Driver domain verification kararı vermez; yalnızca network command'lerini yürütür ve network event'lerini `ProofService`'e iletir.

Driver'ın behaviour seti şunları içerir:

- GossipSub: Availability mesajlarını yaymak.
- Request-response: ProofContent istemek ve göndermek.
- Identify: Peer public key ve network metadata'sını paylaşmak.
- Ping: Bağlantı sağlığını ölçmek.

MVP transport önceliği:

1. QUIC ana transport olarak kullanılır.
2. TCP, Noise ve Yamux fallback olarak desteklenir.
3. İlk discovery yöntemi static bootnode listesidir.

Kademlia tabanlı dynamic discovery, relay ve gelişmiş NAT traversal MVP dışındadır. Peer bağlantısı kurulsa bile availability ve exchange akışlarına katılım için authorization kontrolü tamamlanmalıdır.

## 8. Verification Komponenti

Verification komponenti proof type ile verifier implementation arasındaki registry'yi ve worker'ları içerir.

```text
proof_type -> verifier implementation
```

Verification availability propagation'ı bloklamamalıdır. ProofContent hazır olduğunda task worker'a gönderilir. Worker sonucu ProofService'e iletir:

```text
valid cryptographic proof   -> Verified
invalid cryptographic proof -> Wrong
```

Proof alınamamışsa veya type belirlenemiyorsa worker çalıştırılmaz ve chain query için `UNAVAILABLE` üretilir.

Aynı proof hash'i için tek bir verification task bulunmalıdır. Bunun için per-hash deduplication veya single-flight benzeri bir mekanizma kullanılmalıdır.

## 9. Ephemeral Storage Komponenti

Moka bu sistemde cache olarak kullanılır; permanent database değildir. Ayrıca ayrı bir permanent database kullanılmayacaktır.

### 9.1 Moka Cache

```text
Moka cache
├── proof_hash -> ProofContent
├── proof_hash -> pending ProofSubmission metadata
├── proof_hash -> VerificationStatus
└── proof_hash -> in-flight/failure metadata
```

### 9.2 Rust In-Memory State

```text
Rust maps/RwLock
├── active chain proof hash set
├── availability index
├── provider request state
└── verification task state
```

Proof lookup Moka cache'ten yapılır; ihtiyaç halinde process içindeki in-memory proof map'e düşülebilir. Proof byte'ları büyük olabileceği için cache policy açıkça tanımlanmalıdır:

- Maximum capacity.
- TTL/TTI.
- Entry weight veya byte size.
- Eviction metriği.

Moka eviction veya process restart proof state'ini kaybettirebilir. Bu tasarımda bu kabul edilebilir:

- Chain sonraki status query'lerinde güncel hash setini tekrar gönderir.
- Availability index peer announcement'ları ile yeniden oluşur.
- Restart sonrası node proof'u yeniden edindiğinde announcement yayınlar.

## 10. Identity ve Peer Authorization

MVP'de libp2p identity doğrudan `priv_validator_key.json` içindeki consensus key'den türetilebilir.

Peer authorization şu soruya cevap verir:

```text
Bu PeerId aktif validator setindeki bir validator'a mı ait?
```

Kontrol akışı:

1. Peer Identify public key gönderir.
2. Public key'den PeerId tekrar türetilir.
3. Türetilen PeerId bağlantıdaki PeerId ile karşılaştırılır.
4. Public key local CometBFT validator set'iyle karşılaştırılır.
5. Başarılıysa peer availability ve exchange protocol'lerine dahil edilir.

Başlangıç validator seti alınamazsa P2P fail-closed davranır. Authorized olmayan peer availability veya proof exchange akışlarına dahil edilmez.

Bu doğrudan key reuse modeli MVP içindir. İleride:

```text
dedicated libp2p key
        +
consensus-key signed attestation
        +
chain ID / expiry / validator identity
```

delegation modeline geçilebilir. Bu değişiklik P2P driver'ın transport sorumluluğunu değil, identity provider ve authorization implementation'ını değiştirmelidir.

## 11. Runtime ve Komponentler Arası İletişim

Application runtime şu komponentleri başlatır:

```text
App
├── ProofService
├── Chain relayer/status client
├── P2pDriver
├── AvailabilityIndex
├── Moka cache
├── Verification workers
└── RPC server
```

Komponentler doğrudan birbirlerinin iç state'ine erişmek yerine command/event channel'ları ve service API'lerini kullanmalıdır.

Örnek akış:

```text
RPC Submit
    -> ProofService
        -> cache proof
        -> publish availability
        -> chain relayer

Chain Status Query
    -> ProofService
        -> active hash set update
        -> local lookup
        -> availability lookup/query
        -> exchange request
        -> verification schedule
        -> state response

P2P Exchange Response
    -> ProofService
        -> validate content
        -> cache proof
        -> availability announcement
        -> verification schedule if hash is active
```

P2P driver doğrudan verifier registry'ye veya storage policy'lerine karar vermez. Network event üretir; ProofService domain kararını verir.

### 11.1 Process Lifecycle CLI

Verifier foreground service olarak çalışır ve process lifecycle iki komutla yönetilir:

```text
pulsar-verifier run  --config config/default.toml
pulsar-verifier stop --config config/default.toml
```

`run`, config'teki Unix domain socket'i bind eder ve gelecekteki P2P/RPC task'larının sahibi olan `App` runtime'ını başlatır. Process daemonize edilmez; systemd, container runtime veya başka bir process manager tarafından foreground process olarak yönetilebilir.

`stop`, aynı config'i okuyarak control socket'e `shutdown` command'i gönderir. Runtime socket request'i, `Ctrl-C` ve `SIGTERM` aynı cancellation akışında birleşir. Component task'ları grace period içinde kapandıktan sonra control socket kaldırılır ve process success ile çıkar.

Control socket local process yönetimi içindir; public RPC surface'in parçası değildir. Socket parent directory'si yalnız mevcut kullanıcı tarafından erişilebilir olmalı, aktif instance'ın socket'i overwrite edilmemeli ve yalnız current user'a ait stale socket güvenli biçimde temizlenmelidir.

## 12. Uçtan Uca Senaryolar

Bu bölüm komponentlerin farklı durumlarda nasıl davrandığını tanımlar.

### 12.1 Geçerli İlk RPC Submission

Consumer proof bytes ve signed `TxRaw` gönderir.

```text
RPC
  -> TxRaw/MsgSubmitProof decode
  -> proof hash binding validation
  -> supported proof type validation
  -> temporary cache
  -> AvailabilityAnnouncement
  -> chain broadcast
  -> Pending response
```

Bu noktada cryptographic verification yapılmaz. Proof hash'i chain'in sonraki status request'inde görünürse processing gate açılır.

### 12.2 Bozuk Transaction veya Hash Mismatch

Aşağıdaki durumlardan biri gerçekleşirse submission reddedilir:

- TxRaw veya TxBody decode edilemiyor.
- Beklenen type URL bulunmuyor.
- Birden fazla Cosmos message var.
- `MsgSubmitProof` decode edilemiyor.
- Hash 32 byte değil.
- `proof_type` boş veya unsupported.
- Transaction hash'i ile `BLAKE3(proof)` eşleşmiyor.

Sonuç:

```text
reject
  -> no cache
  -> no announcement
  -> no chain broadcast
```

Bu durum `WRONG` verification state'i değildir; proof verification aşamasına hiç ulaşılmamıştır.

### 12.3 Chain Hash Seti Proof Local'deyken Gelirse

Chain verifier'a hash setini gönderir ve ilgili proof local cache'te bulunur.

```text
active hash set
    -> local ProofContent found
    -> select verifier by proof_type
    -> schedule verification
```

Verifier sonucu:

- Cryptographic verification başarılıysa `VERIFIED`.
- Cryptographic verification başarısızsa `WRONG`.

### 12.4 Chain Hash Seti Gelir, Proof Local'de Yoktur, Provider Bilinir

Verifier hash'i active set'e alır, availability index'te provider bulur ve doğrudan exchange request gönderir.

```text
active hash set
    -> local miss
    -> provider found
    -> GetProofRequest(provider)
    -> ProofContent validation
    -> cache + announcement
    -> verification
```

Provider `ProofNotFound` dönerse provider map'ten çıkarılır ve başka provider denenebilir. Hiçbir provider'dan geçerli içerik alınamazsa chain query için `UNAVAILABLE` döndürülür.

### 12.5 Chain Hash Seti Gelir, Provider Bilinmez

Verifier local proof bulamaz ve availability index'te provider yoktur.

```text
active hash set
    -> local miss
    -> no provider known
    -> AvailabilityQuery gossip
    -> AvailabilityResponse(provider_peer_ids)
    -> provider seç
    -> GetProofRequest
```

Query'yi gören peer yalnızca kendi availability map'inde bildiği provider'ları response eder. Response'u gönderen peer'ın proof sahibi olması zorunlu değildir.

### 12.6 Download Edilen Proof'un Hash'i Yanlışsa

Peer geçerli görünen fakat istenen hash ile eşleşmeyen content gönderirse:

```text
response
    -> requested hash mismatch veya BLAKE3 mismatch
    -> discard content
    -> provider güvenilirliğini düşür
    -> no verification
```

Bu durum `WRONG` state'i değildir. `WRONG`, yalnızca doğru hash'e sahip proof'un cryptographic verifier tarafından geçersiz bulunmasıdır.

### 12.7 Proof Hash Doğru, ZK Proof Yanlışsa

Proof bytes hash ile eşleşir, ancak verifier proof'u cryptographically geçersiz bulur.

```text
valid content binding
    -> verification
    -> cryptographic failure
    -> WRONG
```

Bu proof local cache'te tutulabilir; ancak state `WRONG` olarak işaretlenir. Aynı proof için tekrar tekrar verification başlatılmamalıdır.

### 12.8 RPC Proof'u Chain Hash Setinden Önce Gelirse

Proof local cache'e alınır ve announcement yayınlanır. Hash henüz active chain setinde olmadığı için verification veya başka peer'dan retrieval yapılmaz.

```text
RPC submission
    -> OffChainAvailable
    -> announcement only
    -> wait for chain hash set
```

Chain daha sonra hash'i gönderdiğinde proof `Available` durumuna geçer ve verification başlatılır.

### 12.9 Chain Aynı Hash Setini Tekrar Gönderirse

Status query'leri idempotent işlenir:

- Active set membership tekrar güncellenir.
- Tamamlanmış verification tekrar başlatılmaz.
- In-flight exchange veya verification task duplicate edilmez.
- Mevcut state tekrar döndürülür.

### 12.10 Process Restart Olursa

Permanent database olmadığı için process restart sonrası Moka ve in-memory state kaybolabilir.

Restart sonrası:

1. Chain status query ile active hash setini tekrar gönderir.
2. Node elinde proof yoksa availability index'ten provider bulmaya çalışır.
3. Provider bilinmiyorsa AvailabilityQuery yayınlar.
4. Proof yeniden edinildiğinde announcement yayınlanır.
5. Proof alındıktan sonra verification schedule edilir.

## 13. Güvenlik ve Sınırlar

MVP'de zorunlu kontroller:

- Peer identity doğrulaması.
- Active validator set membership kontrolü.
- Gossipsub mesaj boyutu limiti.
- Request-response proof boyutu limiti.
- Proof hash ve transaction hash eşleşmesi.
- Unknown proof type reddi.
- Transaction içinde beklenmeyen ek mesajların reddi.
- Availability response source peer ve provider ID authorization kontrolü.
- Download edilen ProofContent'in tekrar doğrulanması.
- Proof exchange request'inin yalnızca active chain hash setinden sonra başlatılması.
- Aynı hash için duplicate verification engeli.
- Tekrar gelen status request'lerinin idempotent işlenmesi.

Availability announcement proof doğrulaması değildir. Bir peer'ın “bende var” demesi, proof'un doğru olduğu anlamına gelmez.

## 14. Implementation Sırası

Implementation aşağıdaki sırayla ilerlemelidir:

1. Proto contract'larını oluşturmak:
   - `ProofSubmission` ve `ProofContent`.
   - Placeholder `MsgSubmitProof`.
   - Availability mesajları.
   - Proof exchange mesajları.
   - RPC request/response mesajları.
2. Domain proof types ve lifecycle state'lerini oluşturmak.
3. Moka cache ve in-memory storage abstraction'larını yazmak.
4. Placeholder verifier registry ve worker abstraction'ını eklemek.
5. `ProofSubmission` validation ve `ProofService` akışını yazmak.
6. Chain relayer/status client'ı yazmak.
7. P2P identity ve validator authorization'ı yazmak.
8. GossipSub availability protocol'ünü yazmak.
9. Request-response proof exchange'i yazmak.
10. RPC submit ve batch chain status query'lerini bağlamak.
11. Runtime orchestration ve controlled shutdown'ı tamamlamak.
12. İki veya daha fazla local node ile integration test yapmak.

Her aşamada önce contract ve unit test yazılmalı, sonra bir sonraki katmana geçilmelidir.

## 15. MVP Dışında Kalanlar

İlk MVP şu özellikleri kapsamaz:

- Signed identity delegation veya attestation.
- Kademlia veya dynamic peer discovery.
- Relay, hole punching ve gelişmiş NAT traversal.
- Production-grade peer scoring.
- Sophisticated bandwidth/rate limiting policy.
- Kalıcı distributed database replication.
- Birden fazla provider arasında gelişmiş scheduling.
- Chain reorganization için kapsamlı rollback stratejisi.
- Gerçek Groth16 ve Kimchi verifier implementation detayları.

MVP'nin temel protokol sınırları değişmeden bu özellikler sonraki iterasyonlarda eklenebilir.
