//! Ollama Client - Integration with Ollama LLM server

use serde::{Deserialize, Serialize};
use anyhow::{Result, Context};
use reqwest::Client;

/// Ollama API client
pub struct OllamaClient {
    base_url: String,
    model: String,
    client: Client,
}

#[derive(Serialize)]
struct OllamaRequest {
    model: String,
    prompt: String,
    stream: bool,
    format: Option<String>, // JSON format for structured output
    options: Option<OllamaOptions>, // Generation options
}

#[derive(Serialize)]
struct OllamaOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    num_predict: Option<u32>, // Max tokens to generate (default is usually 128, increase for longer JSON)
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>, // Temperature for sampling
}

#[derive(Deserialize, Debug)]
struct OllamaResponse {
    response: String,
    done: bool,
}

impl OllamaClient {
    /// Create a new Ollama client
    pub fn new(base_url: Option<String>, model: Option<String>) -> Self {
        Self {
            base_url: base_url.unwrap_or_else(|| "http://localhost:11434".to_string()),
            model: model.unwrap_or_else(|| "llama3.2".to_string()),
            client: Client::new(),
        }
    }
    
    /// Generate a completion
    pub async fn generate(&self, prompt: &str, json_format: bool) -> Result<String> {
        let url = format!("{}/api/generate", self.base_url);
        
        let mut request = OllamaRequest {
            model: self.model.clone(),
            prompt: prompt.to_string(),
            stream: false,
            format: None,
            options: Some(OllamaOptions {
                num_predict: Some(4096), // Increase max tokens for complete JSON responses (was 2048)
                temperature: Some(0.1), // Lower temperature for more deterministic JSON
            }),
        };
        
        if json_format {
            request.format = Some("json".to_string());
        }
        
        let response = self.client
            .post(&url)
            .json(&request)
            .send()
            .await
            .context("Failed to send request to Ollama")?;
        
        let ollama_response: OllamaResponse = response
            .json()
            .await
            .context("Failed to parse Ollama response")?;
        
        Ok(ollama_response.response)
    }
    
    /// Generate structured JSON output
    pub async fn generate_json<T>(&self, prompt: &str) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let response = self.generate(prompt, true).await?;
        
        // Try to extract JSON from response (might be wrapped in markdown code blocks)
        let json_str = Self::extract_json_from_response(&response);
        
        // Try to parse JSON - if it fails, show more context
        let parsed: T = match serde_json::from_str(&json_str) {
            Ok(p) => p,
            Err(e) => {
                // Try to find where the JSON might be truncated or malformed
                let error_msg = if json_str.len() > 1000 {
                    format!("{}... (truncated, full length: {})", &json_str[..1000], json_str.len())
                } else {
                    json_str.clone()
                };
                return Err(anyhow::anyhow!("Failed to parse JSON response from Ollama: {}\n\nResponse was:\n{}", e, error_msg));
            }
        };
        
        Ok(parsed)
    }
    
    /// Extract JSON from response (handles markdown code blocks)
    fn extract_json_from_response(response: &str) -> String {
        let trimmed = response.trim();
        
        // Check if wrapped in markdown code block
        if trimmed.starts_with("```json") {
            if let Some(end) = trimmed[7..].find("```") {
                return trimmed[7..7+end].trim().to_string();
            }
        } else if trimmed.starts_with("```") {
            if let Some(start) = trimmed.find('\n') {
                if let Some(end) = trimmed[start+1..].find("```") {
                    return trimmed[start+1..start+1+end].trim().to_string();
                }
            }
        }
        
        // Try to find JSON object/array boundaries
        if let Some(start) = trimmed.find('{') {
            if let Some(end) = trimmed.rfind('}') {
                return trimmed[start..=end].to_string();
            }
        } else if let Some(start) = trimmed.find('[') {
            if let Some(end) = trimmed.rfind(']') {
                return trimmed[start..=end].to_string();
            }
        }
        
        // Return as-is if no extraction needed
        trimmed.to_string()
    }
    
    /// Check if Ollama server is available
    pub async fn health_check(&self) -> Result<bool> {
        let url = format!("{}/api/tags", self.base_url);
        match self.client.get(&url).send().await {
            Ok(resp) => Ok(resp.status().is_success()),
            Err(_) => Ok(false),
        }
    }
}

impl Default for OllamaClient {
    fn default() -> Self {
        Self::new(None, None)
    }
}

