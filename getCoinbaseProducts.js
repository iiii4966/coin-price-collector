const axios = require('axios');

async function getCoinbaseProducts() {
    try {
        const response = await axios.get('https://api.exchange.coinbase.com/products');
        const products = response.data;

        const filteredProducts = products.filter(product => 
            product.quote_currency === 'USD' && 
            (product.status === 'online' || product.status === 'offline')
        );

        console.log(`필터링된 상품 수: ${filteredProducts.length}`);
        console.log('상품 코드:');
        filteredProducts.forEach(product => console.log(product.id));
    } catch (error) {
        console.error('API 요청 중 오류 발생:', error.message);
    }
}

getCoinbaseProducts();
