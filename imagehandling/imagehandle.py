import re

from PIL import Image
from pytesseract import pytesseract

from instagramstories.logs.logs_config import log


class ImageHandling:
    def __init__(self, path_to_image: str) -> None:
        self.path_to_image = path_to_image

    def extract_text_from_image(self) -> str:
        try:
            # Open image with Pillow
            image = Image.open(self.path_to_image)
        except Exception as e:
            log.logger.warning(e)

        text = ''

        try:
            # Extract text from image
            text = pytesseract.image_to_string(image, lang='eng+kaz+rus+ara+chi_tra+deu+fra+ita+jpn+kor+por+equ+fin')
            text = text.replace('\n', '')
        except Exception as e:
            log.logger.warning(e)

        if text:
            try:
                # Escape from garbage symbols
                text = text.rstrip(' ')
                text = ''.join(re.sub(r'[^a-zA-ZА-Яа-я0-9\s]', '', text)).strip()
            except Exception as e:
                log.logger.warning(e)

        return text if len(text) > 0 else 'empty'
