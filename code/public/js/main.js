  var ansi = new AnsiUp();

  var CONTINUE_STATES = ["prep", "que", "run"];
  var last_done = 0;
  var last_file_start = 0;
  var last_fragment = "";

  window.onscroll = updateFilePos;
  window.onresize = updateFilePos;

  function updateFilePos() {
    // Set the filepos indicators
    var filepos_top = document.getElementById("filepos-top");
    var filepos_bot = document.getElementById("filepos-bot");
    var filepos_end = document.getElementById("filepos-end");

    var ansitxt = document.getElementById("ansitxt");
    var lines = ansitxt.innerHTML.split("\n").length - 1;
    var height = document.body.scrollHeight;

    var line_height = height / lines;
    var top_line = Math.floor(document.body.scrollTop / line_height);
    var view_lines = Math.floor(document.body.clientHeight / line_height);

    filepos_top.innerHTML = 1 + top_line;
    filepos_bot.innerHTML = 1 + top_line + view_lines;
    filepos_end.innerHTML = lines;
  }

  function process(txt) {

    if (txt.length == 0) return;

    var block = ansi.ansi_to_html(ansi.linkify(ansi.escape_for_html(txt)));
    if (block) {
      // console.log("block: " + block);
      var ansitxt = document.getElementById("ansitxt");
      ansitxt.innerHTML += block;
    }

    // Scroll to the bottom if the box is checked
    var tail = document.getElementById("filepos-tail");
    if (tail && tail.checked) {
      window.scrollTo(0, document.body.scrollHeight);
    }

    // Whether we scroll or not, update the position and line count
    updateFilePos();
  }

  function checkState() {
    var xmlhttp = new XMLHttpRequest();
    xmlhttp.open("GET", config.state_path, true);
    xmlhttp.setRequestHeader("Cache-Control", "no-cache");
    xmlhttp.onreadystatechange = function() {
      if (xmlhttp.readyState == 4 && xmlhttp.status == 200) {
        // console.log(xmlhttp.responseText);
        var state = JSON.parse(xmlhttp.responseText)[0];
        if (CONTINUE_STATES.indexOf(state) != -1) {
          window.setTimeout(function () { nextChunk(); }, 1000);
        } else {
          last_done = last_done + 1;
          if (last_done < 2) {
            window.setTimeout(function () { nextChunk(); }, 1000);
          } else {
            console.log("Completed loading file.");
          }
        }
      }
      else if (xmlhttp.readyState == 4) {
        // Try again in 1 second
        window.setTimeout(function () { checkState(); }, 1000);
      }
    };
    xmlhttp.send();
  }

  function nextChunk() {
    var xmlhttp = new XMLHttpRequest();
    xmlhttp.open("GET", config.tail_path, true);
    var range_query = "bytes=" + last_file_start + "-" ;
    // console.log("Range: " + range_query);
    xmlhttp.setRequestHeader("Range", range_query);
    xmlhttp.setRequestHeader("Cache-Control", "no-cache");
    xmlhttp.onreadystatechange = function() {
      if (xmlhttp.readyState == 4 && xmlhttp.status == 206) {
        process(xmlhttp.response);
        var range = xmlhttp.getResponseHeader("Content-Range");
        var range_parts = range.match(/bytes (\d+)-(\d+)\/(\d+)/);
        // console.log(range_parts);
        var r_start = parseInt(range_parts[1]);
        var r_max_end = parseInt(range_parts[2]);
        var r_actual_end = parseInt(range_parts[3]);

        last_file_start = 1 + (r_max_end < r_actual_end ? r_max_end : r_actual_end);
        window.setTimeout(function () { nextChunk(); }, 100);
        last_done = 0;
      }
      else if (xmlhttp.readyState == 4) {
        // Try again in 1 second
        window.setTimeout(function () { checkState(); }, 1000);
      }
    };
    xmlhttp.send();
  };

  nextChunk();
