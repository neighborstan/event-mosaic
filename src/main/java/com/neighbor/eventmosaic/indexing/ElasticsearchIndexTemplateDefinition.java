package com.neighbor.eventmosaic.indexing;

/**
 * Версионированные шаблоны фиксированных индексов GDELT.
 */
enum ElasticsearchIndexTemplateDefinition {

	EVENTS(
			"gdelt-events-v1-template",
			"elasticsearch/gdelt-events-v1-template.json"),

	MENTIONS(
			"gdelt-mentions-v1-template",
			"elasticsearch/gdelt-mentions-v1-template.json");

	private final String templateName;
	private final String resourcePath;

	ElasticsearchIndexTemplateDefinition(String templateName, String resourcePath) {
		this.templateName = templateName;
		this.resourcePath = resourcePath;
	}

	String templateName() {
		return templateName;
	}

	String resourcePath() {
		return resourcePath;
	}

}
