<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Raw data downloads extracted from Wiktionary</title>
  <link rel="canonical" href="https://kaikki.org/dictionary/rawdata.html"/>
  <link rel="shortcut icon" type="image/png" href="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAABmJLR0QA/wD/AP+gvaeTAAAAmElEQVQ4y7WSSw4CQQhECxYuZjRcwftfyjO00YUby40khBkcHCOr/tCPohrg16CBNHArp7rTTgEZkAqiXZW7FQCADEgCnkoADVOs6I8T5LoA0HB8L++dFmiYaWBUoJ6YJRdxkwGJgPHFY/jP6J7Zib5oPsz90iBrqjxP18gJ8nRQaw6KamcHtQeJhkPYXipz9YNJj+w4/hEvzrs/aYZwHLoAAAAASUVORK5CYII=">
  
  <style>
body {
   margin-left: 20px;
   margin-right: 20px;
   font-family: "Times New Roman", Times, serif;
   font-size: 20px;
   font-weight: 400;
}

.formsandsounds {
  padding-left: 20px;
  font-size: 18px;
}

li {
  padding-bottom: 3px;
}

ol>li {
  padding-bottom: 10px;
}

.formsandsounds li {
  padding: 0;
}

.gloss {
  font-weight: 600;
  font-size: 110%;
}

.info {
  display: block;
  padding-bottom: 5px;
}

.infolabel {
}

.infodetail {
  font-style: italic;
}

.glossinfo {
  display: block;
  padding-left: 10px;
  font-size: 18px;
}

.glossinfo li {
  padding-bottom: 0;
}

.trlang {
  font-weight: 600;
}

.inaccurate {
  font-size: 75%;
}

:target .glosses {
  color: red;
}

.nondisambiguated {
  padding-left: 20px;
  font-size: 18px;
}

.nondisambiguatedh {
  font-weight: 600;
  font-size: 18px;
}

.nondisambiguatedb {
  padding-left: 20px;
}

ul.breadcrumb {
  /* padding: 10px 16px; */
  padding: 0;
  margin: 0;
  list-style: none;
  /* background-color: #eee; */
}

ul.breadcrumb li {
  display: inline;
  font-size: 18px;
}

ul.breadcrumb li+li:before {
  padding: 8px;
  color: black;
  content: "\00bb";
}

ul.breadcrumb li a {
  color: #0275d8;
  text-decoration: none;
}

ul.breadcrumb li a:hover {
  color: #01447e;
  text-decoration: underline;
}

.indented {
  margin-left: 40px;
  padding-bottom: 10px;
}

.search {
    float: right;
    max-width: 180px;
}

.search form {
    position: relative;
    padding: 0px;
    margin: 0px;
    border-radius: 3px;
}

.searchField {
    width: 100%;
}

.searchButton {
    position: absolute;
    top: 2px;
    bottom: 2px;
    right: 2px;
    min-width: 28px;
    width: 28px;
    height: auto;
    cursor: pointer;
    white-space: nowrap;
    overflow: hidden;
    border: 0;
    padding: 0;
    z-index: 1;
}

.searchResults {
    display: none;
    position: relative;
    top: 0px;
    padding: 5px;
    border: 1px solid #888;
    border-radius: 3px;
}

.searchResultItem {
    background-color: white;
    color: black;
    text-decoration: initial;
    width: 100%;
    display: block;
}

.searchResultSelected {
    background-color: black;
    color: white;
    text-decoration: initial;
    width: 100%;
    display: block;
}

.stacktrace {
    font-size: 18px;
}

.errorpath {
    display: block;
    font-size: 18px;
    padding-top: 0;
    margin-top: 0;
}

.hideable {
    padding-top: 3px;
    padding-bottom: 3px;
}

.close-ck, .close-content, .close-hide {
    display: none;
}

.close-ck:checked ~ .close-content {
    display: block;
}

.close-ck:checked ~ .close-hide {
    display: inline;
}

.close-ck:checked ~ .close-show {
    display: none;
}

.close-show, .close-hide {
    background-color: #e0e0e0;
}

.audio-play {
    border: 0;
    padding: 0;
    background-color: transparent;
}

th {
    text-align: left;
    background: #cccccc;
}

tr:nth-child(odd) {
    background: #e8e8e8;
}

tr:nth-child(even) {
    background: #ffffff;
}

.inflections td {
    padding-right: 10px;
}

.inflections_lang_list table {
    border: none;
}

div.jsonpadding div.jsonpadding {
    padding-left: 40px;
}

div.jsonfont {
   font-family: monospace, sans-serif;
   font-size: 18px;
   font-weight: 400;
}

div.jsonfont a {
    text-decoration: none;
    font-weight: bold;
}

div.jsonfont strong.field a{
    color: light-blue;
    font-weight: normal;
    }

div.jsonfont a.tag {
    color: green;
    }

div.jsonfont em.lang {
    }


span.overflow {
    background: #bbbbff;
}

tr :nth-child(1) {
    text-align: right;
    padding-right: 10px;
}

tr :nth-child(3) {
    text-align: right;
    padding-right: 10px;
}

tr :nth-child(5) {
    text-align: right;
    padding-right: 10px;
}

td.errors {
    background: #ffaaaa;
}

.de-edition h1 {
    background: #aaffaa;
}

.ru-edition h1 {
    background: #ffaaff;
}

.fr-edition h1 {
    background: #aaaaff;
}

.es-edition h1 {
    background: #ffffaa;
}

.zh-edition h1 {
    background: #aaffff;
}

  </style>
</head>
<body class="en-edition">
<div class="search">
  <form>
      <input id="searchField" class="searchField" type="search" name="search"
             placeholder="Search enwiktionary"
             autocapitalize="none" disabled
             title="Search the dictionary [Alt+Shift+f]"
             accesskey="f" autocomplete="off">
      <input type="submit" name="Go" value="Go"
             id="searchButton" class="searchButton" disabled
             title="Go to a page with this exact name">
  </form>
  <div id="searchResults" class="searchResults">
  </div>
</div>

<h1>Raw data downloads extracted from Wiktionary</h1>
<ul class="breadcrumb"><li><a href="https://kaikki.org/index.html">Home</a></li><li><a href="index.html">English edition</a></li><li>Raw data</li></ul>

<script type="application/ld+json">
{
  "@context": "https://schema.org",
  "@type": "BreadcrumbList",
  "itemListElement": [
  {
     "@type": "ListItem",
     "position": 1,
     "item": { "@id": "https://kaikki.org/index.html", "name": "Home" }
  },
  {
     "@type": "ListItem",
     "position": 2,
     "item": { "@id": "index.html", "name": "English%20edition" }
  },
  {
     "@type": "ListItem",
     "position": 3,
     "item": { "@id": "dictionary/rawdata.html", "name": "Raw%20data" }
  }
  ]
}
</script>
<p>This page contains download links for the raw data extracted from Wiktionary using Wiktextract.  This data is updated regularly (usually at least once a week). <h3>English-language edition of Wiktionary</h3>The current version was extracted from the <a href="https://dumps.wikimedia.org/backup-index.html">enwiktionary dump</a> dated 2026-07-06.  It contains data for hundreds of languages, and has glosses and other metadata in English. The data formats are documented at <a href="https://github.com/tatuylonen/wiktextract">https://github.com/tatuylonen/wiktextract</a>.</p>

<ul>
<li><a href="raw-wiktextract-data.jsonl" download>Download raw Wiktextract data (JSONL, one object per line)</a> (23.1GB) or <a href="raw-wiktextract-data.jsonl.gz" download>compressed .gz</a> (2.6GB)</li>
<li><a href="wiktextract-error-data.json" download>Download Wiktextract error data (JSON, one large object)</a> (865.2MB) or <a href="wiktextract-error-data.json.gz" download>compressed .gz</a> (38.3MB)</li>
<li><a href="wiktionary-modules.tar" download>Download all Wiktionary Lua modules (.tar)</a> (459.8MB) or <a href="wiktionary-modules.tar.gz" download>compressed .gz</a> (87.0MB)</li>
<li><a href="wiktionary-templates.tar" download>Download all Wiktionary templates (.tar)</a> (135.5MB) or <a href="wiktionary-templates.tar.gz" download>compressed .gz</a> (11.7MB)</li>
<li><a href="wiktionary-audios.tar" download>Wiktionary audio files bulk download (.tar)</a> (20.4GB)</li>
<ul><li>Note: this includes about 99.5% of the audio files (those with redirects in Wikimedia Commons are currently missing).  About 942,000 files are included.  This set is not yet automatically updated.  File names are the last URL component (see <code>ogg_url</code> and <code>mp3_url</code> fields in the Wiktextract data).</li></ul>

</ul>
<p><strong><a href="https://github.com/tatuylonen/wiktextract/issues/1178">DEPRECATED</a>, will be removed in the near future</strong>: For post-processed data, please look at the download links at the end of the main page for each language (or the page for all languages combined) under <a href="/dictionary/">https://kaikki.org/dictionary/</a>.</p>
<h3>Raw downloads for other Wiktionary editions</h3>
<p>Because each different edition of Wiktionary requires a lot of work so that Wiktextract can process it, there are still only a few other editions that are currently supported. These are currently work in progress.</p>
<ul>
<li><a href="downloads/zh/zh-extract.jsonl">Chinese zh-extract.jsonl (1.8GB)</a></li>
<li><a href="downloads/zh/zh-extract.jsonl.gz">Chinese zh-extract.jsonl.gz (compressed 215.1MB)</a></li>
<li><a href="downloads/zh/zh-extract.log">Chinese zh-extract.log (20.6MB)</a></li>
<li><a href="downloads/zh/zh-extract.log.gz">Chinese zh-extract.log.gz (compressed 2.5MB)</a></li>
<li><a href="downloads/zh/zh-extract.errors">Chinese zh-extract.errors (49.2MB)</a></li>
<li><a href="downloads/zh/zh-extract.errors.gz">Chinese zh-extract.errors.gz (compressed 2.5MB)</a></li>
<li><a href="downloads/cs/cs-extract.jsonl">Czech cs-extract.jsonl (264.0MB)</a></li>
<li><a href="downloads/cs/cs-extract.jsonl.gz">Czech cs-extract.jsonl.gz (compressed 36.6MB)</a></li>
<li><a href="downloads/cs/cs-extract.log">Czech cs-extract.log (295.5kB)</a></li>
<li><a href="downloads/cs/cs-extract.log.gz">Czech cs-extract.log.gz (compressed 45.3kB)</a></li>
<li><a href="downloads/cs/cs-extract.errors">Czech cs-extract.errors (955.0kB)</a></li>
<li><a href="downloads/cs/cs-extract.errors.gz">Czech cs-extract.errors.gz (compressed 65.8kB)</a></li>
<li><a href="downloads/nl/nl-extract.jsonl">Dutch nl-extract.jsonl (1.1GB)</a></li>
<li><a href="downloads/nl/nl-extract.jsonl.gz">Dutch nl-extract.jsonl.gz (compressed 121.0MB)</a></li>
<li><a href="downloads/nl/nl-extract.log">Dutch nl-extract.log (12.2MB)</a></li>
<li><a href="downloads/nl/nl-extract.log.gz">Dutch nl-extract.log.gz (compressed 487.7kB)</a></li>
<li><a href="downloads/nl/nl-extract.errors">Dutch nl-extract.errors (9.8MB)</a></li>
<li><a href="downloads/nl/nl-extract.errors.gz">Dutch nl-extract.errors.gz (compressed 350.0kB)</a></li>
<li><a href="downloads/fr/fr-extract.jsonl">French fr-extract.jsonl (6.2GB)</a></li>
<li><a href="downloads/fr/fr-extract.jsonl.gz">French fr-extract.jsonl.gz (compressed 675.9MB)</a></li>
<li><a href="downloads/fr/fr-extract.log">French fr-extract.log (2.1MB)</a></li>
<li><a href="downloads/fr/fr-extract.log.gz">French fr-extract.log.gz (compressed 168.3kB)</a></li>
<li><a href="downloads/fr/fr-extract.errors">French fr-extract.errors (3.7MB)</a></li>
<li><a href="downloads/fr/fr-extract.errors.gz">French fr-extract.errors.gz (compressed 146.0kB)</a></li>
<li><a href="downloads/de/de-extract.jsonl">German de-extract.jsonl (2.8GB)</a></li>
<li><a href="downloads/de/de-extract.jsonl.gz">German de-extract.jsonl.gz (compressed 286.5MB)</a></li>
<li><a href="downloads/de/de-extract.log">German de-extract.log (7.3MB)</a></li>
<li><a href="downloads/de/de-extract.log.gz">German de-extract.log.gz (compressed 864.6kB)</a></li>
<li><a href="downloads/de/de-extract.errors">German de-extract.errors (14.2MB)</a></li>
<li><a href="downloads/de/de-extract.errors.gz">German de-extract.errors.gz (compressed 977.9kB)</a></li>
<li><a href="downloads/el/el-extract.jsonl">Greek el-extract.jsonl (1.4GB)</a></li>
<li><a href="downloads/el/el-extract.jsonl.gz">Greek el-extract.jsonl.gz (compressed 100.9MB)</a></li>
<li><a href="downloads/el/el-extract.log">Greek el-extract.log (3.0MB)</a></li>
<li><a href="downloads/el/el-extract.log.gz">Greek el-extract.log.gz (compressed 327.2kB)</a></li>
<li><a href="downloads/el/el-extract.errors">Greek el-extract.errors (3.7MB)</a></li>
<li><a href="downloads/el/el-extract.errors.gz">Greek el-extract.errors.gz (compressed 214.7kB)</a></li>
<li><a href="downloads/id/id-extract.jsonl">Indonesian id-extract.jsonl (28.5MB)</a></li>
<li><a href="downloads/id/id-extract.jsonl.gz">Indonesian id-extract.jsonl.gz (compressed 2.7MB)</a></li>
<li><a href="downloads/id/id-extract.log">Indonesian id-extract.log (366.9kB)</a></li>
<li><a href="downloads/id/id-extract.log.gz">Indonesian id-extract.log.gz (compressed 50.8kB)</a></li>
<li><a href="downloads/id/id-extract.errors">Indonesian id-extract.errors (888.5kB)</a></li>
<li><a href="downloads/id/id-extract.errors.gz">Indonesian id-extract.errors.gz (compressed 62.8kB)</a></li>
<li><a href="downloads/it/it-extract.jsonl">Italian it-extract.jsonl (488.3MB)</a></li>
<li><a href="downloads/it/it-extract.jsonl.gz">Italian it-extract.jsonl.gz (compressed 38.0MB)</a></li>
<li><a href="downloads/it/it-extract.log">Italian it-extract.log (145.8kB)</a></li>
<li><a href="downloads/it/it-extract.log.gz">Italian it-extract.log.gz (compressed 22.6kB)</a></li>
<li><a href="downloads/it/it-extract.errors">Italian it-extract.errors (272.1kB)</a></li>
<li><a href="downloads/it/it-extract.errors.gz">Italian it-extract.errors.gz (compressed 21.2kB)</a></li>
<li><a href="downloads/ja/ja-extract.jsonl">Japanese ja-extract.jsonl (397.0MB)</a></li>
<li><a href="downloads/ja/ja-extract.jsonl.gz">Japanese ja-extract.jsonl.gz (compressed 58.0MB)</a></li>
<li><a href="downloads/ja/ja-extract.log">Japanese ja-extract.log (4.4MB)</a></li>
<li><a href="downloads/ja/ja-extract.log.gz">Japanese ja-extract.log.gz (compressed 568.6kB)</a></li>
<li><a href="downloads/ja/ja-extract.errors">Japanese ja-extract.errors (13.6MB)</a></li>
<li><a href="downloads/ja/ja-extract.errors.gz">Japanese ja-extract.errors.gz (compressed 693.0kB)</a></li>
<li><a href="downloads/ko/ko-extract.jsonl">Korean ko-extract.jsonl (182.9MB)</a></li>
<li><a href="downloads/ko/ko-extract.jsonl.gz">Korean ko-extract.jsonl.gz (compressed 24.6MB)</a></li>
<li><a href="downloads/ko/ko-extract.log">Korean ko-extract.log (1.3MB)</a></li>
<li><a href="downloads/ko/ko-extract.log.gz">Korean ko-extract.log.gz (compressed 151.3kB)</a></li>
<li><a href="downloads/ko/ko-extract.errors">Korean ko-extract.errors (2.2MB)</a></li>
<li><a href="downloads/ko/ko-extract.errors.gz">Korean ko-extract.errors.gz (compressed 155.7kB)</a></li>
<li><a href="downloads/ku/ku-extract.jsonl">Kurdish ku-extract.jsonl (732.3MB)</a></li>
<li><a href="downloads/ku/ku-extract.jsonl.gz">Kurdish ku-extract.jsonl.gz (compressed 63.2MB)</a></li>
<li><a href="downloads/ku/ku-extract.log">Kurdish ku-extract.log (15.4MB)</a></li>
<li><a href="downloads/ku/ku-extract.log.gz">Kurdish ku-extract.log.gz (compressed 1004.5kB)</a></li>
<li><a href="downloads/ku/ku-extract.errors">Kurdish ku-extract.errors (72.5MB)</a></li>
<li><a href="downloads/ku/ku-extract.errors.gz">Kurdish ku-extract.errors.gz (compressed 2.5MB)</a></li>
<li><a href="downloads/ms/ms-extract.jsonl">Malay ms-extract.jsonl (41.2MB)</a></li>
<li><a href="downloads/ms/ms-extract.jsonl.gz">Malay ms-extract.jsonl.gz (compressed 5.6MB)</a></li>
<li><a href="downloads/ms/ms-extract.log">Malay ms-extract.log (3.2MB)</a></li>
<li><a href="downloads/ms/ms-extract.log.gz">Malay ms-extract.log.gz (compressed 273.6kB)</a></li>
<li><a href="downloads/ms/ms-extract.errors">Malay ms-extract.errors (4.1MB)</a></li>
<li><a href="downloads/ms/ms-extract.errors.gz">Malay ms-extract.errors.gz (compressed 262.9kB)</a></li>
<li><a href="downloads/pl/pl-extract.jsonl">Polish pl-extract.jsonl (975.0MB)</a></li>
<li><a href="downloads/pl/pl-extract.jsonl.gz">Polish pl-extract.jsonl.gz (compressed 124.0MB)</a></li>
<li><a href="downloads/pl/pl-extract.log">Polish pl-extract.log (2.6MB)</a></li>
<li><a href="downloads/pl/pl-extract.log.gz">Polish pl-extract.log.gz (compressed 243.6kB)</a></li>
<li><a href="downloads/pl/pl-extract.errors">Polish pl-extract.errors (4.9MB)</a></li>
<li><a href="downloads/pl/pl-extract.errors.gz">Polish pl-extract.errors.gz (compressed 229.2kB)</a></li>
<li><a href="downloads/pt/pt-extract.jsonl">Portuguese pt-extract.jsonl (329.8MB)</a></li>
<li><a href="downloads/pt/pt-extract.jsonl.gz">Portuguese pt-extract.jsonl.gz (compressed 33.6MB)</a></li>
<li><a href="downloads/pt/pt-extract.log">Portuguese pt-extract.log (2.7MB)</a></li>
<li><a href="downloads/pt/pt-extract.log.gz">Portuguese pt-extract.log.gz (compressed 403.8kB)</a></li>
<li><a href="downloads/pt/pt-extract.errors">Portuguese pt-extract.errors (6.5MB)</a></li>
<li><a href="downloads/pt/pt-extract.errors.gz">Portuguese pt-extract.errors.gz (compressed 468.1kB)</a></li>
<li><a href="downloads/ru/ru-extract.jsonl">Russian ru-extract.jsonl (2.3GB)</a></li>
<li><a href="downloads/ru/ru-extract.jsonl.gz">Russian ru-extract.jsonl.gz (compressed 275.8MB)</a></li>
<li><a href="downloads/ru/ru-extract.log">Russian ru-extract.log (2.1MB)</a></li>
<li><a href="downloads/ru/ru-extract.log.gz">Russian ru-extract.log.gz (compressed 232.5kB)</a></li>
<li><a href="downloads/ru/ru-extract.errors">Russian ru-extract.errors (4.6MB)</a></li>
<li><a href="downloads/ru/ru-extract.errors.gz">Russian ru-extract.errors.gz (compressed 260.5kB)</a></li>
<li><a href="downloads/simple/simple-extract.jsonl">Simple English simple-extract.jsonl (35.3MB)</a></li>
<li><a href="downloads/simple/simple-extract.jsonl.gz">Simple English simple-extract.jsonl.gz (compressed 4.4MB)</a></li>
<li><a href="downloads/simple/simple-extract.log">Simple English simple-extract.log (5.0kB)</a></li>
<li><a href="downloads/simple/simple-extract.errors">Simple English simple-extract.errors (7.6kB)</a></li>
<li><a href="downloads/es/es-extract.jsonl">Spanish es-extract.jsonl (1.1GB)</a></li>
<li><a href="downloads/es/es-extract.jsonl.gz">Spanish es-extract.jsonl.gz (compressed 95.8MB)</a></li>
<li><a href="downloads/es/es-extract.log">Spanish es-extract.log (998.2kB)</a></li>
<li><a href="downloads/es/es-extract.log.gz">Spanish es-extract.log.gz (compressed 117.4kB)</a></li>
<li><a href="downloads/es/es-extract.errors">Spanish es-extract.errors (2.4MB)</a></li>
<li><a href="downloads/es/es-extract.errors.gz">Spanish es-extract.errors.gz (compressed 132.7kB)</a></li>
<li><a href="downloads/th/th-extract.jsonl">Thai th-extract.jsonl (1.5GB)</a></li>
<li><a href="downloads/th/th-extract.jsonl.gz">Thai th-extract.jsonl.gz (compressed 66.8MB)</a></li>
<li><a href="downloads/th/th-extract.log">Thai th-extract.log (14.1MB)</a></li>
<li><a href="downloads/th/th-extract.log.gz">Thai th-extract.log.gz (compressed 912.5kB)</a></li>
<li><a href="downloads/th/th-extract.errors">Thai th-extract.errors (18.2MB)</a></li>
<li><a href="downloads/th/th-extract.errors.gz">Thai th-extract.errors.gz (compressed 830.6kB)</a></li>
<li><a href="downloads/tr/tr-extract.jsonl">Turkish tr-extract.jsonl (665.3MB)</a></li>
<li><a href="downloads/tr/tr-extract.jsonl.gz">Turkish tr-extract.jsonl.gz (compressed 40.2MB)</a></li>
<li><a href="downloads/tr/tr-extract.log">Turkish tr-extract.log (627.2MB)</a></li>
<li><a href="downloads/tr/tr-extract.log.gz">Turkish tr-extract.log.gz (compressed 6.5MB)</a></li>
<li><a href="downloads/tr/tr-extract.errors">Turkish tr-extract.errors (114.8MB)</a></li>
<li><a href="downloads/tr/tr-extract.errors.gz">Turkish tr-extract.errors.gz (compressed 1.0MB)</a></li>
<li><a href="downloads/vi/vi-extract.jsonl">Vietnamese vi-extract.jsonl (260.4MB)</a></li>
<li><a href="downloads/vi/vi-extract.jsonl.gz">Vietnamese vi-extract.jsonl.gz (compressed 31.1MB)</a></li>
<li><a href="downloads/vi/vi-extract.log">Vietnamese vi-extract.log (22.5MB)</a></li>
<li><a href="downloads/vi/vi-extract.log.gz">Vietnamese vi-extract.log.gz (compressed 1.5MB)</a></li>
<li><a href="downloads/vi/vi-extract.errors">Vietnamese vi-extract.errors (37.5MB)</a></li>
<li><a href="downloads/vi/vi-extract.errors.gz">Vietnamese vi-extract.errors.gz (compressed 1.3MB)</a></li>
</ul>
<hr/>
<div>

<p>This page is a part of the kaikki.org machine-readable  dictionary.  This dictionary is based on structured data extracted on 2026-07-25 from the enwiktionary dump dated 2026-07-06 using <a href="https://github.com/tatuylonen/wiktextract">wiktextract</a> (<a href="https://github.com/tatuylonen/wiktextract/commit/d9fa2335957c9089ce2c3fb110a075cf072903da">d9fa233</a> and <a href="https://github.com/tatuylonen/wikitextprocessor/commit/9e92f4b53a98748f849ef6186617535abb0fca7b">9e92f4b</a>).
<p>If you use this data in academic research, please cite Tatu Ylonen: <a href="http://www.lrec-conf.org/proceedings/lrec2022/pdf/2022.lrec-1.140.pdf">Wiktextract: Wiktionary as Machine-Readable Structured Data</a>, Proceedings of the 13th Conference on Language Resources and Evaluation (LREC), pp. 1317-1325, Marseille, 20-25 June 2022.  Linking to the relevant page(s) under https://kaikki.org would also be greatly appreciated.</p>
</p>
</div>

<script >
var search_cache = null;
var search_selection = 0;
var search_query = null;

var wikt_edition = "en";

String.prototype.isUpper = function () {
    var char = this[0];
    var ret = false;
    if (char == char.toUpperCase()) {
        ret = true;
    }
    return ret;
}
String.prototype.isLower = function () {
    var char = this[0];
    var ret = false;
    if (char == char.toLowerCase()) {
        ret = true;
    }
    return ret;
}

function add_result(results, v, url, i) {
    var a = document.createElement("a");
    if (results.childElementCount == search_selection) {
        a.setAttribute("class", "searchResultSelected");
    } else {
        a.setAttribute("class", "searchResultItem");
    }
    a.href = url;
    a.textContent = v;
    function enter() {
        results.children.item(search_selection).setAttribute(
            "class", "searchResultItem");
        search_selection = i;
        results.children.item(i).setAttribute(
            "class", "searchResultSelected");
    }
    a.onmouseenter = enter;
    results.appendChild(a);
}

function finalize_search(results, query, lst, api_search=null) {
    /*console.log("finalize_search", query, lst); */
    var max_cnt = 10;
    var lcq = query.toLowerCase();
    var matches = [];
    for (var i = 0; i < lst.length; i++) {
        var title = lst[i][0];
        if (title.toLowerCase().substring(0, lcq.length) == lcq) {
            var url = lst[i][1];
            matches.push([lcq, results, title, url, i]);
        }
    }
    function cmp_lowercase_first(a, b) {
        var ret = a[0][0].localeCompare(b[0][0]);
        if (ret != 0) {
             return ret;
        }
        if (a[2][0].isLower() && b[2][0].isUpper()) {
            return -1;
        } else if (a[2][0].isUpper() && b[2][0].isLower()) {
            return 1;
        }
        return 0;
    }
    function cmp_uppercase_first(a, b) {
        var ret = a[0][0].localeCompare(b[0][0]);
        if (ret != 0) {
             return ret;
        }
        if (a[2][0].isLower() && b[2][0].isUpper()) {
            return 1;
        } else if (a[2][0].isUpper() && b[2][0].isLower()) {
            return -1;
        }
        return 0;
    }
    var cmp_fn;
    if (query[0].isUpper()) {
        cmp_fn = cmp_uppercase_first;
    } else {
        cmp_fn = cmp_lowercase_first;
    }

    matches.sort(cmp_fn);
    if (matches.length > 0 && typeof(api_search) == "string") {
        window.location.href = matches[0][3]
    }
    var cnt = 0;
    for (var i = 0; i < matches.length; i++) {
        var item = matches[i];
        add_result(item[1], item[2], item[3], item[4]);
        cnt += 1;
        if (cnt >= max_cnt) {
            break;
        }
    }

    results.style.display = "block";
}

function search_fill(api_search=null) {
    var query_field = document.getElementById("searchField");
    var results = document.getElementById("searchResults");
    var query;
    // if doing an api search, use api_search
    // otherwise use field value
    if (typeof(api_search) == "string") {
        query = api_search;
    } else {
        query = query_field.value;
    }

    if (query != search_query) {
        search_selection = 0;
        search_query = query;
    }
    results.style.display = "none";
    results.innerHTML = "";

    if (query == "") {
        return;
    }
    var prefix_len;
    if (search_cache && search_cache[2]) {
        prefix_len = 3;
    } else {
        prefix_len = 2;
    }

    var prefix = query.toLowerCase().substring(0, prefix_len);
    if (search_cache && search_cache[0] == prefix) {
        /*console.log("from cache"); */
        finalize_search(results, query, search_cache[1], api_search);
    } else {
        var fn = prefix;
        fn = fn.replace("/", "_slash_");
        fn = fn.replace("\\", "_backslash_");
        fn = fn.replace("*", "_star_");
        fn = fn.replace("?", "_ques_");
        fn = fn.replace("#", "_hash_");
        fn = fn.replace(".", "_dot_");
        fn = encodeURIComponent(fn) + ".json";
        var url;
        if (wikt_edition == "en") {
            url = "/dictionary/search/start/" + fn;
        } else {
            url = "/" + wikt_edition + "wiktionary/search/start/" + fn;
        }
        fetch(url)
            .then(function(resp) {
                if (resp.ok) {
                    resp.json().then(function(data) {
                        var three_char = data[0];
                        var lst = data[1];
                        /* console.log("from fetch, three_char: ", three_char);*/

                        search_cache = [prefix, lst, three_char];
                        finalize_search(results, query, lst, api_search);
                    });
                }
            })
            .catch(function(resp) {
                var li = document.createElement("div");
                li.style.color = "red";
                li.textContent = "Search failed due to network error";
                results.appendChild(li);
            });
    }
}

function initialize() {
    /*console.log("initialize"); */
    var query_field = document.getElementById("searchField");
    var query_button = document.getElementById("searchButton");
    var results = document.getElementById("searchResults");
    function keydown(e) {
        e = e || window.event;
        var key = e.key;
        if (!key) {
            var charCode = e.keyCode || e.which;
            key = String.fromCharCode(charCode);
        }
        if (key == "ArrowUp") {
            if (search_selection > 0) {
                search_selection -= 1;
            }
        } else if (key == "ArrowDown") {
            if (search_selection + 1 < results.childElementCount) {
                search_selection += 1;
            }
        } else if (key == "Enter") {
            if (search_cache && search_cache[1].length > 0) {
                results.children.item(search_selection).click();
                return false;
            }
        }
    }
    q = new URLSearchParams(window.location.search);
    if (q.has("q")) {
        search_fill(q.get("q"));
    }
    results.onkeydown = keydown;
    query_field.removeAttribute("disabled");
    query_button.removeAttribute("disabled");
    query_field.onkeydown = keydown;
    query_field.onkeyup = search_fill;
    query_field.onpaste = function() { setTimeout(search_fill, 100); };
    query_button.onsubmit = function(e) {
        if (search_cache && search_cache[1].length > 0) {
            results.children.item(search_selection).click();
            return false;
        }
    };
}
document.addEventListener("DOMContentLoaded", initialize);

</script>
</body>
</html>
