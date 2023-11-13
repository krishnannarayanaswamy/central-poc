class Product:
    def __init__(self, **kwargs):
        self.product_id = kwargs['product_id']
        self.product_name = kwargs['product_name_en']
        self.brand = kwargs['brand']
        self.short_description = kwargs['short_description_en']
        self.long_description = kwargs['long_description_en']
        self.image = kwargs['image']
        self.availability = kwargs['availability']
        self.product_categories = kwargs['product_categories']
        self.sale_price = kwargs['sale_price']
        self.specs = kwargs['specs']
    