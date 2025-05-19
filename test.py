import modal

MODEL_ID = "NousResearch/Meta-Llama-3-8B"
MODEL_REVISION = "315b20096dc791d381d514deb5f8bd9c8d6d3061"

image = modal.Image.debian_slim().pip_install(
    "transformers==4.49.0", "torch==2.6.0", "accelerate==1.4.0"
)

app = modal.App("example-base-Meta-Llama-3-8B", image=image)
GPU_CONFIG = "A100"
CACHE_DIR = "/cache"
cache_vol = modal.Volume.from_name("hf-hub-cache", create_if_missing=True)

@app.cls(
    gpu=GPU_CONFIG,
    volumes={CACHE_DIR: cache_vol},
    scaledown_window=10,
    timeout=60,
)
@modal.concurrent(max_inputs=15)
class Model:
    @modal.enter()
    def setup(self):
        import torch
        from transformers import AutoModelForCausalLM, AutoTokenizer, pipeline
        from huggingface_hub import snapshot_download
        model_path = snapshot_download(repo_id=MODEL_ID, cache_dir=CACHE_DIR)
        print(f"Model downloaded to: {model_path}")
        model = AutoModelForCausalLM.from_pretrained(MODEL_ID, cache_dir=CACHE_DIR)
        tokenizer = AutoTokenizer.from_pretrained(MODEL_ID, cache_dir=CACHE_DIR)
        self.pipeline = pipeline(
            "text-generation",
            model=model,
            tokenizer=tokenizer,
            model_kwargs={"torch_dtype": torch.bfloat16},
            device_map="auto",
        )
    
    @modal.method()
    def generate(self, input: str):
        return self.pipeline(input)

# Point d'entrée explicite
if __name__ == "__main__":
    with app.run():
        model = Model()
        prompt = "Please write a Python function to compute the Fibonacci numbers."
        result = model.generate.remote(prompt)
        print(result)