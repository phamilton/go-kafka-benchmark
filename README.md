# go-kafka-benchmark

To build / execute the "go" version of the benchmarking tool:

`go build && ./benchmark`

To build / execute the "java" version of the benchmarking tool:

`./gradlew run`

### Test Results ###

The following test the raw performance of the clients producer and consumer against different Kafka set ups

<h3>
  <strong>
    <br/>Kafka Scenarios (no auth or SSL)</strong>
</h3>
<table class="relative-table wrapped" style="width: 47.1411%;">
  <colgroup>
    <col style="width: 33.4625%;"/>
    <col style="width: 24.5478%;"/>
    <col style="width: 25.0646%;"/>
    <col style="width: 16.9251%;"/>
  </colgroup>
  <tbody>
    <tr>
      <th>Scenario</th>
      <th colspan="3" style="text-align: center;">events/s</th>
    </tr>
    <tr>
      <th colspan="1">
        <br/>
      </th>
      <th colspan="1" style="text-align: center;">confluent-kafka-go</th>
      <th colspan="1" style="text-align: center;">sarama</th>
      <th colspan="1" style="text-align: center;">java</th>
    </tr>
    <tr>
      <td colspan="1">Producer, Async, 1 Partition</td>
      <td colspan="1" style="text-align: right;">563,043</td>
      <td colspan="1" style="text-align: right;">518,266</td>
      <td colspan="1" style="text-align: right;">1,291,489</td>
    </tr>
    <tr>
      <td colspan="1">Producer, Async, 3 Partitions</td>
      <td colspan="1" style="text-align: right;">523,157</td>
      <td colspan="1" style="text-align: right;">620,495</td>
      <td colspan="1" style="text-align: right;">1,197,461</td>
    </tr>
    <tr>
      <td colspan="1">Producer, Async, 8 Partitions</td>
      <td colspan="1" style="text-align: right;">487,858</td>
      <td colspan="1" style="text-align: right;">645,426</td>
      <td colspan="1" style="text-align: right;">902,810</td>
    </tr>
    <tr>
      <td colspan="1">Consumer, 1 Partition</td>
      <td colspan="1" style="text-align: right;">152,950</td>
      <td colspan="1" style="text-align: right;">1,021,820</td>
      <td colspan="1" style="text-align: right;">1,185,817</td>
    </tr>
    <tr>
      <td colspan="1">Consumer, 3 Partitions</td>
      <td colspan="1" style="text-align: right;">429,697</td>
      <td colspan="1" style="text-align: right;">1,776,931</td>
      <td colspan="1" style="text-align: right;">1,716,738</td>
    </tr>
    <tr>
      <td colspan="1">Consumer, 8 Partitions</td>
      <td colspan="1" style="text-align: right;">902,810</td>
      <td colspan="1" style="text-align: right;">1,819,865</td>
      <td colspan="1" style="text-align: right;">1,652,346</td>
    </tr>
  </tbody>
</table>
<p>
  <br/>
</p>
<h3>
<p>
  <strong>Kafka Scenarios (with SASL and SSL)</strong>
</p>
</h3>
<table class="relative-table wrapped" style="width: 47.1411%;">
  <colgroup>
    <col/>
    <col/>
    <col/>
    <col/>
  </colgroup>
  <tbody>
    <tr>
      <th>Scenario</th>
      <th colspan="3" style="text-align: center;">events/s</th>
    </tr>
    <tr>
      <th colspan="1">
        <br/>
      </th>
      <th colspan="1" style="text-align: center;">confluent-kafka-go</th>
      <th colspan="1" style="text-align: center;">sarama</th>
      <th colspan="1" style="text-align: center;">java</th>
    </tr>
    <tr>
      <td colspan="1">Producer, Async, 1 Partition</td>
      <td colspan="1" style="text-align: right;">560,493</td>
      <td colspan="1" style="text-align: right;">498,402</td>
      <td colspan="1" style="text-align: right;">979,527</td>
    </tr>
    <tr>
      <td colspan="1">Producer, Async, 3 Partitions</td>
      <td colspan="1" style="text-align: right;">470,571</td>
      <td colspan="1" style="text-align: right;">601,849</td>
      <td colspan="1" style="text-align: right;">1,006,441</td>
    </tr>
    <tr>
      <td colspan="1">Producer, Async, 8 Partitions</td>
      <td colspan="1" style="text-align: right;">459,083</td>
      <td colspan="1" style="text-align: right;">596,849</td>
      <td colspan="1" style="text-align: right;">1,145,606</td>
    </tr>
    <tr>
      <td colspan="1">Consumer, 1 Partition</td>
      <td colspan="1" style="text-align: right;">173,514</td>
      <td colspan="1" style="text-align: right;">938,122</td>
      <td colspan="1" style="text-align: right;">1,737,317</td>
    </tr>
    <tr>
      <td colspan="1">Consumer, 3 Partitions</td>
      <td colspan="1" style="text-align: right;">445,867</td>
      <td colspan="1" style="text-align: right;">1,576,031</td>
      <td colspan="1" style="text-align: right;">1,926,782</td>
    </tr>
    <tr>
      <td colspan="1">Consumer, 8 Partitions</td>
      <td colspan="1" style="text-align: right;">1,012,515</td>
      <td colspan="1" style="text-align: right;">1,752,578</td>
      <td colspan="1" style="text-align: right;">1,936,108</td>
    </tr>
  </tbody>
</table>
<p>
  <br/>
</p>
<h3>Azure EventHub Scenarios</h3>
<p>Autoscale to 20 Units</p>
<table class="relative-table wrapped" style="width: 46.9586%;">
  <colgroup>
    <col style="width: 30.7393%;"/>
    <col style="width: 27.3671%;"/>
    <col style="width: 25.0324%;"/>
    <col style="width: 16.8612%;"/>
  </colgroup>
  <tbody>
    <tr>
      <th>Scenario</th>
      <th colspan="3" style="text-align: center;">events/s</th>
    </tr>
    <tr>
      <th colspan="1">
        <br/>
      </th>
      <th colspan="1" style="text-align: center;">confluent-kafka-go</th>
      <th colspan="1" style="text-align: center;">sarama</th>
      <th colspan="1" style="text-align: center;">java</th>
    </tr>
    <tr>
      <td>Producer, Async</td>
      <td style="text-align: right;">39,076</td>
      <td colspan="1" style="text-align: right;">23,346</td>
      <td colspan="1" style="text-align: right;">23,369</td>
    </tr>
    <tr>
      <td colspan="1">Consumer, 4 Partitions</td>
      <td colspan="1" style="text-align: right;">209,163</td>
      <td colspan="1" style="text-align: right;">75,708</td>
      <td colspan="1" style="text-align: right;">92,004</td>
    </tr>
  </tbody>
</table>
<p>
  <br/>
</p>
<p>Autoscale to 4 Units</p>
<table class="relative-table wrapped" style="width: 46.9586%;">
  <colgroup>
    <col style="width: 30.7393%;"/>
    <col style="width: 27.3671%;"/>
    <col style="width: 25.0324%;"/>
    <col style="width: 16.8612%;"/>
  </colgroup>
  <tbody>
    <tr>
      <th>Scenario</th>
      <th colspan="3" style="text-align: center;">events/s</th>
    </tr>
    <tr>
      <th colspan="1">
        <br/>
      </th>
      <th colspan="1" style="text-align: center;">confluent-kafka-go</th>
      <th colspan="1" style="text-align: center;">sarama</th>
      <th colspan="1" style="text-align: center;">java</th>
    </tr>
    <tr>
      <td>Producer, Async</td>
      <td style="text-align: right;">13,667</td>
      <td colspan="1" style="text-align: right;">8,987</td>
      <td colspan="1" style="text-align: right;">3,783</td>
    </tr>
    <tr>
      <td colspan="1">Consumer, 4 Partitions</td>
      <td colspan="1" style="text-align: right;">119,243</td>
      <td colspan="1" style="text-align: right;">56,444</td>
      <td colspan="1" style="text-align: right;">52,216</td>
    </tr>
  </tbody>
</table>
