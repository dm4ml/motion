import clip
import logging
import requests
import torch
import tqdm
from io import BytesIO
from PIL import Image


class OutfitDataset(torch.utils.data.Dataset):
    def __init__(self, img_urls, captions, device):
        _, preprocess_fn = clip.load("ViT-B/32", device=device)

        self.img_urls = img_urls
        self.preprocessed_images = {}
        self.captions = [clip.tokenize(cap).to(device) for cap in captions]

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246"
        }

        for image_url in self.img_urls:
            response = requests.get(image_url, headers=headers)
            self.preprocessed_images[image_url] = preprocess_fn(
                Image.open(BytesIO(response.content))
            )

    def __getitem__(self, index):
        img_url = self.img_urls[index]
        caption = self.captions[index]
        return self.preprocessed_images[img_url], caption

    def __len__(self):
        return len(self.img_urls)


def convert_models_to_fp32(model):
    for p in model.parameters():
        p.data = p.data.float()
        p.grad.data = p.grad.data.float()


def fine_tune_model(model, img_urls, captions, batch_size=8, epochs=5):
    device = "cuda" if torch.cuda.is_available() else "cpu"
    dataset = OutfitDataset(img_urls, captions, device)

    if device == "cpu":
        model.float()

    # Create dataloader
    train_dataloader = torch.utils.data.DataLoader(
        dataset, batch_size=batch_size, shuffle=True
    )

    # Initialize loss and optimizer
    loss_img = torch.nn.CrossEntropyLoss()
    loss_txt = torch.nn.CrossEntropyLoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=1e-5)
    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(
        optimizer, len(train_dataloader) * epochs
    )
    best_loss = float("inf")

    # Train model
    for epoch in range(epochs):
        model.train()
        pbar = tqdm(train_dataloader, leave=False)
        step = 0
        epoch_loss = 0

        for images, captions in pbar:
            step += 1
            images = images.to(device)
            captions = captions.to(device)

            optimizer.zero_grad()

            image_logits, text_logits = model(images, captions)

            loss = loss_img(
                image_logits, torch.arange(batch_size).to(device)
            ) + loss_txt(text_logits, torch.arange(batch_size).to(device))
            loss.backward()
            epoch_loss += loss.item()

            if device != "cpu":
                convert_models_to_fp32(model)

            optimizer.step()
            scheduler.step()

            if device != "cpu":
                clip.model.convert_weights(model)

            pbar.set_description(
                f"Train batch loss: {loss.item()}", refresh=True
            )

        epoch_loss /= step
        logging.info(f"Epoch {epoch} loss: {epoch_loss}")
        if epoch_loss > best_loss:
            break

        best_loss = epoch_loss

    # Return model
    return model
