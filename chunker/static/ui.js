function api_call(path, data, callback) {
	$.ajax({
		"dataType": "json",
		"url": "/api/" + path + ".json",
		"data": data
	}).done(function(data) {
		$("#debug").text("{0}: {1} ({2})".format(path, data["status"], data["message"] || ''));
		callback(data);
	});
}

function update_state() {
	api_call(
		"state", {},
		function(data) {
			var text = "<table>";
			$.each(data.repos, function(i, repo) {
				text = text + "<tr>" +
					"<td>"+
						"{0} ({1})".format(repo.name, repo.type) +
						"<br><small>{0}</small>".format(repo.root) +
						"<br><small>{0}</small>".format(repo.uuid) +
					"</td>" +
					"<td>" +
						"<a href='javascript: remove(\"{0}\");'>Remove</a>".format(repo.uuid) +
					"</td>" +
				"</tr>";
			});
			text = text + "</table>";
			$("#state").html(text);
		}
	);
}

function remove(uuid) {
	api_call(
		"remove", {"uuid": uuid},
		function(data) {
			// update_state();
		}
	);
}

$(function() {

	update_state();
	setInterval(update_state, 10*1000);

});
