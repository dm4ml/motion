import motion
import platform
import torch

from PIL import Image
from io import BytesIO

from diffusers import StableDiffusionImg2ImgPipeline


class ExtractOutfit(motion.Trigger):
    def routes(self):
        return [
            motion.Route(
                namespace="closet",
                key="img_blob",
                infer=self.removeBackgroundAndEnhance,
                fit=None,
            )
        ]

    def setUp(self, cursor):
        # Set up the stable diffusion model
        # pip install diffusers transformers accelerate scipy safetensors
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        if "arm" in platform.processor():
            device = "mps"
        model_id_or_path = "runwayml/stable-diffusion-v1-5"
        pipe = StableDiffusionImg2ImgPipeline.from_pretrained(
            model_id_or_path, torch_dtype=torch.float16
        )
        pipe.to(device)
        pipe.enable_attention_slicing()

        return {"device": device, "model": pipe}

    def removeBackgroundAndEnhance(self, cursor, triggered_by):
        img_blob = triggered_by.value

        # Convert the image blob to a PIL image
        image = Image.open(BytesIO(img_blob)).convert("RGB")  # SD params
        image.thumbnail((512, 512), Image.ANTIALIAS)

        prompt = "a photo of an outfit with no background"
        images = self.state["model"](
            prompt=prompt, image=image, strength=0.5, guidance_scale=7.5
        ).images

        # Convert PIL image to image blob
        byte_stream = BytesIO()
        images[0].save(byte_stream, format="PNG")
        bytes_value = byte_stream.getvalue()

        # Put the image blob in the database
        cursor.set(
            namespace=triggered_by.namespace,
            identifier=triggered_by.identifier,
            key_values={"sd_img_blob": bytes_value},
        )
