console.log("draw.js")

const fetchButton = document.getElementById('fetch')
fetchButton.addEventListener("click", function () {
  var xhr = new XMLHttpRequest()
  // xhr.setRequestHeader("Content-Type", "application/json")
  xhr.open("GET", "/api/etfs/1/changes/", true)
  xhr.send()
  xhr.onreadystatechange = function () {
    if (xhr.readyState == 4) {
      console.log(JSON.parse(xhr.responseText))
    }
  }

})